package gitserver

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"golang.org/x/crypto/ssh"
)

// loadOrCreateHostKey returns the server's host key, generating an ed25519
// key on first run. The file is in OpenSSH format, so ssh-keygen -lf prints
// the same fingerprint the daemon logs. The key has to persist: clients pin
// it in known_hosts on first connect and refuse a server whose key changed.
func loadOrCreateHostKey(path string) (signer ssh.Signer, created bool, err error) {
	data, err := os.ReadFile(path)
	if err == nil {
		signer, err = ssh.ParsePrivateKey(data)
		if err != nil {
			return nil, false, fmt.Errorf("%s: %w", path, err)
		}
		return signer, false, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, false, err
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, false, err
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		return nil, false, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, false, err
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		return nil, false, err
	}
	signer, err = ssh.NewSignerFromKey(priv)
	if err != nil {
		return nil, false, err
	}
	return signer, true, nil
}
