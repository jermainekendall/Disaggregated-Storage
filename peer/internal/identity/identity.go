// Package identity manages the peer's persistent Ed25519 libp2p key and the
// small state files the program keeps next to its config.
package identity

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/libp2p/go-libp2p/core/crypto"
)

// LoadOrCreateKey reads the identity key, generating and saving one (0600) on
// first use. The peer ID derives from it, so it must never change.
func LoadOrCreateKey(path string) (crypto.PrivKey, error) {
	raw, err := os.ReadFile(path)
	if err == nil {
		if st, _ := os.Stat(path); st != nil && st.Mode().Perm()&0o077 != 0 {
			return nil, fmt.Errorf("identity key %s has mode %o; it must be 0600", path, st.Mode().Perm())
		}
		return crypto.UnmarshalPrivateKey(raw)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		return nil, err
	}
	raw, err = crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	// O_EXCL: never overwrite a key another process just created.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Write(raw); err != nil {
		return nil, err
	}
	return priv, f.Sync()
}

// ReadJSON loads a state file into v. A missing file leaves v unchanged.
func ReadJSON(path string, v interface{}) error {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

// WriteJSON saves v atomically with owner-only permissions.
func WriteJSON(path string, v interface{}) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
