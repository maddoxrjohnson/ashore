package gitserver

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/crypto/ssh"
)

func TestHostKeyCreatedThenLoaded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys", "host_ed25519")
	first, created, err := loadOrCreateHostKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("first call did not report creating the key")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("host key mode = %o, want 600", perm)
	}
	second, created, err := loadOrCreateHostKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("second call generated a new key instead of loading the file")
	}
	if a, b := ssh.FingerprintSHA256(first.PublicKey()), ssh.FingerprintSHA256(second.PublicKey()); a != b {
		t.Fatalf("fingerprint changed across loads: %s then %s", a, b)
	}
}

func TestHostKeyCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host_ed25519")
	if err := os.WriteFile(path, []byte("not a key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadOrCreateHostKey(path); err == nil {
		t.Fatal("corrupt key file was accepted")
	}
}
