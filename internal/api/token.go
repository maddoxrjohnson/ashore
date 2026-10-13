package api

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// TokenFile is the admin token's name inside the data directory.
const TokenFile = "admin.token"

// LoadToken returns the admin token, creating it on the first run: 32
// random bytes as hex, in a file only the daemon's user can read. The value
// itself is never logged; the operator reads the file.
func LoadToken(dataDir string) (string, error) {
	path := filepath.Join(dataDir, TokenFile)
	b, err := os.ReadFile(path)
	if err == nil {
		token := strings.TrimSpace(string(b))
		if token == "" {
			return "", fmt.Errorf("%s is empty; remove it to generate a new token", path)
		}
		return token, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	token := hex.EncodeToString(raw)
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	slog.Info("admin token created", "path", path)
	return token, nil
}

// ReadToken reads an existing token file, for commands that run beside the
// daemon on the same machine.
func ReadToken(dataDir string) (string, error) {
	path := filepath.Join(dataDir, TokenFile)
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("admin token: %w (has the daemon run once?)", err)
	}
	return strings.TrimSpace(string(b)), nil
}
