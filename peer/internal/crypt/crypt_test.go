package crypt

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

func init() {
	// Keep tests fast; real wrappings use the defaults.
	argonTime, argonMemory = 1, 8*1024
}

func TestMasterKeyWrap(t *testing.T) {
	mk, _ := NewKey()
	blob, err := WrapMasterKey(mk, "correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnwrapMasterKey(blob, "correct horse battery")
	if err != nil || !bytes.Equal(got, mk) {
		t.Fatalf("unwrap: %v", err)
	}
	if _, err := UnwrapMasterKey(blob, "wrong passphrase!"); err != ErrWrongPassphrase {
		t.Fatalf("wrong passphrase: %v", err)
	}
	blob[25] ^= 1 // tamper with KDF parameters
	if _, err := UnwrapMasterKey(blob, "correct horse battery"); err == nil {
		t.Fatal("tampered header accepted")
	}
	if _, err := WrapMasterKey(mk, "short"); err == nil {
		t.Fatal("short passphrase accepted")
	}
}

func TestFileKeyBoundToFileID(t *testing.T) {
	mk, _ := NewKey()
	fk, _ := NewKey()
	w, _ := WrapFileKey(mk, fk, "file-a")
	if got, err := UnwrapFileKey(mk, w, "file-a"); err != nil || !bytes.Equal(got, fk) {
		t.Fatalf("unwrap: %v", err)
	}
	if _, err := UnwrapFileKey(mk, w, "file-b"); err == nil {
		t.Fatal("file key accepted for another file")
	}
}

func TestStreamRoundTripSizes(t *testing.T) {
	key, _ := NewKey()
	for _, size := range []int{0, 1, chunkSize - 1, chunkSize, chunkSize + 1, 3*chunkSize + 17, 5 * chunkSize} {
		plain := make([]byte, size)
		rand.Read(plain)
		var enc bytes.Buffer
		w, _ := NewEncryptWriter(key, &enc)
		w.Write(plain)
		w.Close()
		r, err := NewDecryptReader(key, &enc)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(r)
		if err != nil || !bytes.Equal(got, plain) {
			t.Fatalf("size %d: %v", size, err)
		}
	}
}

func TestStreamDetectsTampering(t *testing.T) {
	key, _ := NewKey()
	plain := make([]byte, 3*chunkSize+5)
	rand.Read(plain)
	var enc bytes.Buffer
	w, _ := NewEncryptWriter(key, &enc)
	w.Write(plain)
	w.Close()
	ct := enc.Bytes()

	cases := map[string][]byte{
		"truncated at chunk boundary": ct[:11+2*(chunkSize+tagSize)],
		"flipped bit":                 flip(ct, 100),
		"extra bytes":                 append(append([]byte{}, ct...), 1, 2, 3),
	}
	for name, c := range cases {
		r, err := NewDecryptReader(key, bytes.NewReader(c))
		if err == nil {
			_, err = io.ReadAll(r)
		}
		if err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	other, _ := NewKey()
	r, _ := NewDecryptReader(other, bytes.NewReader(ct))
	if _, err := io.ReadAll(r); err == nil {
		t.Error("wrong key: no error")
	}
}

func flip(b []byte, i int) []byte {
	c := append([]byte{}, b...)
	c[i] ^= 0x80
	return c
}

func TestEncryptFileCompresses(t *testing.T) {
	key, _ := NewKey()
	plain := bytes.Repeat([]byte("orchestrator "), 50000)
	var enc bytes.Buffer
	if _, err := EncryptFile(key, bytes.NewReader(plain), &enc); err != nil {
		t.Fatal(err)
	}
	if enc.Len() >= len(plain)/10 {
		t.Fatalf("encrypted size %d, expected compression of %d", enc.Len(), len(plain))
	}
	var out bytes.Buffer
	if _, err := DecryptFile(key, &enc, &out); err != nil || !bytes.Equal(out.Bytes(), plain) {
		t.Fatalf("decrypt: %v", err)
	}
}

func TestNames(t *testing.T) {
	k, _ := NewKey()
	c, _ := EncryptName(k, "holiday.jpg")
	if n, err := DecryptName(k, c); err != nil || n != "holiday.jpg" {
		t.Fatalf("%q %v", n, err)
	}
}
