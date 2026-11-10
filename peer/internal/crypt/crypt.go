// Package crypt holds the client-side encryption.
//
// Keys:
//   - The master key is 32 random bytes. It is stored on the server only in
//     wrapped form: AES-256-GCM under a key derived from the user's passphrase
//     with Argon2id and a random salt. The passphrase itself is never stored.
//   - Each file has its own random 32-byte key, wrapped with the master key
//     (the file ID is authenticated as associated data) and stored on the server.
//
// File contents are gzip-compressed, then encrypted with a chunked
// AES-256-GCM stream (the STREAM construction): 64 KiB chunks, each with a
// nonce made of a random 7-byte prefix, a 4-byte chunk counter and a
// final-chunk flag. Reordering, truncating or extending the stream fails
// authentication. Files of any size are processed without loading them into
// memory.
package crypt

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/argon2"
)

const (
	KeySize   = 32
	chunkSize = 64 << 10
	tagSize   = 16
)

// Argon2id parameters for new master-key wrappings. They are stored in the
// wrapped blob, so they can be raised later without breaking old blobs.
var (
	argonTime    uint32 = 3
	argonMemory  uint32 = 64 * 1024 // KiB
	argonThreads uint8  = 4
)

var (
	magicMaster = []byte("OMK1")
	magicStream = []byte("ORC1")

	// ErrWrongPassphrase is returned when the master key cannot be unwrapped.
	ErrWrongPassphrase = errors.New("wrong passphrase (master key could not be decrypted)")
)

// MinPassphraseLength is the shortest passphrase accepted.
const MinPassphraseLength = 8

// NewKey returns 32 random bytes.
func NewKey() ([]byte, error) {
	k := make([]byte, KeySize)
	_, err := rand.Read(k)
	return k, err
}

func gcm(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func seal(key, plaintext, aad []byte) ([]byte, error) {
	a, err := gcm(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, a.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return a.Seal(nonce, nonce, plaintext, aad), nil
}

func open(key, sealed, aad []byte) ([]byte, error) {
	a, err := gcm(key)
	if err != nil {
		return nil, err
	}
	if len(sealed) < a.NonceSize()+tagSize {
		return nil, errors.New("ciphertext too short")
	}
	return a.Open(nil, sealed[:a.NonceSize()], sealed[a.NonceSize():], aad)
}

// WrapMasterKey encrypts the master key under a key derived from passphrase.
// Layout: "OMK1" | salt(16) | time(4) | memoryKiB(4) | threads(1) | nonce+ciphertext.
func WrapMasterKey(master []byte, passphrase string) ([]byte, error) {
	if len(passphrase) < MinPassphraseLength {
		return nil, fmt.Errorf("passphrase must be at least %d characters", MinPassphraseLength)
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	kek := argon2.IDKey([]byte(passphrase), salt, argonTime, argonMemory, argonThreads, KeySize)
	var hdr bytes.Buffer
	hdr.Write(magicMaster)
	hdr.Write(salt)
	binary.Write(&hdr, binary.BigEndian, argonTime)
	binary.Write(&hdr, binary.BigEndian, argonMemory)
	hdr.WriteByte(argonThreads)
	// The header (including the KDF parameters) is authenticated.
	ct, err := seal(kek, master, hdr.Bytes())
	if err != nil {
		return nil, err
	}
	return append(hdr.Bytes(), ct...), nil
}

// UnwrapMasterKey reverses WrapMasterKey.
func UnwrapMasterKey(blob []byte, passphrase string) ([]byte, error) {
	const hdrLen = 4 + 16 + 4 + 4 + 1
	if len(blob) < hdrLen || !bytes.Equal(blob[:4], magicMaster) {
		return nil, errors.New("not a wrapped master key")
	}
	salt := blob[4:20]
	t := binary.BigEndian.Uint32(blob[20:24])
	m := binary.BigEndian.Uint32(blob[24:28])
	p := blob[28]
	if t == 0 || t > 20 || m < 8*1024 || m > 4*1024*1024 || p == 0 {
		return nil, errors.New("wrapped master key has unreasonable KDF parameters")
	}
	kek := argon2.IDKey([]byte(passphrase), salt, t, m, p, KeySize)
	master, err := open(kek, blob[hdrLen:], blob[:hdrLen])
	if err != nil {
		return nil, ErrWrongPassphrase
	}
	return master, nil
}

// WrapFileKey encrypts a file key with the master key, bound to the file ID.
func WrapFileKey(master, fileKey []byte, fileID string) ([]byte, error) {
	return seal(master, fileKey, []byte("file-key:"+fileID))
}

// UnwrapFileKey reverses WrapFileKey.
func UnwrapFileKey(master, wrapped []byte, fileID string) ([]byte, error) {
	k, err := open(master, wrapped, []byte("file-key:"+fileID))
	if err != nil {
		return nil, fmt.Errorf("file key for %s could not be decrypted: %w", fileID, err)
	}
	return k, nil
}

// EncryptName encrypts a file name with the file key.
func EncryptName(fileKey []byte, name string) ([]byte, error) {
	return seal(fileKey, []byte(name), []byte("file-name"))
}

// DecryptName reverses EncryptName.
func DecryptName(fileKey, sealed []byte) (string, error) {
	b, err := open(fileKey, sealed, []byte("file-name"))
	return string(b), err
}

// ---------------------------------------------------------------- streams

type streamWriter struct {
	aead    cipher.AEAD
	w       io.Writer
	prefix  [7]byte
	counter uint32
	buf     []byte
	closed  bool
}

func (s *streamWriter) nonce(last bool) []byte {
	n := make([]byte, 12)
	copy(n, s.prefix[:])
	binary.BigEndian.PutUint32(n[7:11], s.counter)
	if last {
		n[11] = 1
	}
	return n
}

func (s *streamWriter) flush(last bool) error {
	if s.counter == ^uint32(0) {
		return errors.New("stream too long")
	}
	out := s.aead.Seal(nil, s.nonce(last), s.buf, nil)
	s.counter++
	s.buf = s.buf[:0]
	_, err := s.w.Write(out)
	return err
}

func (s *streamWriter) Write(p []byte) (int, error) {
	if s.closed {
		return 0, errors.New("write after close")
	}
	n := 0
	for len(p) > 0 {
		take := chunkSize - len(s.buf)
		if take > len(p) {
			take = len(p)
		}
		s.buf = append(s.buf, p[:take]...)
		p = p[take:]
		n += take
		// Only flush a full chunk once more data arrives, so the final
		// chunk is always the one written by Close with the last flag.
		if len(s.buf) == chunkSize && len(p) > 0 {
			if err := s.flush(false); err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

func (s *streamWriter) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.flush(true)
}

// NewEncryptWriter returns a writer that encrypts everything written to it
// with key and writes the result to w. Close must be called to write the
// final chunk.
func NewEncryptWriter(key []byte, w io.Writer) (io.WriteCloser, error) {
	a, err := gcm(key)
	if err != nil {
		return nil, err
	}
	s := &streamWriter{aead: a, w: w, buf: make([]byte, 0, chunkSize)}
	if _, err := rand.Read(s.prefix[:]); err != nil {
		return nil, err
	}
	if _, err := w.Write(append(append([]byte{}, magicStream...), s.prefix[:]...)); err != nil {
		return nil, err
	}
	return s, nil
}

type streamReader struct {
	aead    cipher.AEAD
	r       *bufio.Reader
	prefix  [7]byte
	counter uint32
	plain   []byte
	done    bool
}

func (s *streamReader) Read(p []byte) (int, error) {
	for len(s.plain) == 0 {
		if s.done {
			return 0, io.EOF
		}
		chunk := make([]byte, chunkSize+tagSize)
		n, err := io.ReadFull(s.r, chunk)
		if err != nil && err != io.ErrUnexpectedEOF {
			if err == io.EOF {
				return 0, errors.New("encrypted stream truncated (no final chunk)")
			}
			return 0, err
		}
		chunk = chunk[:n]
		_, peekErr := s.r.Peek(1)
		last := peekErr == io.EOF
		nonce := make([]byte, 12)
		copy(nonce, s.prefix[:])
		binary.BigEndian.PutUint32(nonce[7:11], s.counter)
		if last {
			nonce[11] = 1
		}
		plain, err := s.aead.Open(nil, nonce, chunk, nil)
		if err != nil {
			return 0, errors.New("encrypted stream failed authentication (corrupt, truncated or wrong key)")
		}
		s.counter++
		s.plain = plain
		s.done = last
	}
	n := copy(p, s.plain)
	s.plain = s.plain[n:]
	return n, nil
}

// NewDecryptReader returns a reader that decrypts a stream written by
// NewEncryptWriter. Any tampering produces an error, never wrong data.
func NewDecryptReader(key []byte, r io.Reader) (io.Reader, error) {
	a, err := gcm(key)
	if err != nil {
		return nil, err
	}
	br := bufio.NewReaderSize(r, chunkSize+tagSize+1)
	hdr := make([]byte, 11)
	if _, err := io.ReadFull(br, hdr); err != nil || !bytes.Equal(hdr[:4], magicStream) {
		return nil, errors.New("not an encrypted orchestrator stream")
	}
	s := &streamReader{aead: a, r: br}
	copy(s.prefix[:], hdr[4:])
	return s, nil
}

// EncryptFile compresses and encrypts src into dst. It returns the number of
// plaintext bytes read.
func EncryptFile(key []byte, src io.Reader, dst io.Writer) (int64, error) {
	enc, err := NewEncryptWriter(key, dst)
	if err != nil {
		return 0, err
	}
	gz, _ := gzip.NewWriterLevel(enc, gzip.DefaultCompression)
	n, err := io.Copy(gz, src)
	if err != nil {
		return n, err
	}
	if err := gz.Close(); err != nil {
		return n, err
	}
	return n, enc.Close()
}

// DecryptFile reverses EncryptFile.
func DecryptFile(key []byte, src io.Reader, dst io.Writer) (int64, error) {
	dec, err := NewDecryptReader(key, src)
	if err != nil {
		return 0, err
	}
	gz, err := gzip.NewReader(dec)
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(dst, gz)
	if err != nil {
		return n, err
	}
	return n, gz.Close()
}

// HashFile returns the hex SHA-256 of r.
func HashReader(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

// AuditProof returns SHA-256(challenge || data), the proof-of-storage answer.
func AuditProof(challenge []byte, data io.Reader) ([]byte, error) {
	h := sha256.New()
	h.Write(challenge)
	if _, err := io.Copy(h, data); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}
