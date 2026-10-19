package api

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// TokenFile is the admin token's name inside the data directory.
const TokenFile = "admin.token"

// LoadToken returns the admin token, creating it on the first run: 32
// random bytes as hex, in a file only the daemon's user can read. created
// says whether this call made it; the value itself is never logged, the
// operator reads the file.
func LoadToken(dataDir string) (token string, created bool, err error) {
	path := filepath.Join(dataDir, TokenFile)
	b, err := os.ReadFile(path)
	if err == nil {
		token := strings.TrimSpace(string(b))
		if token == "" {
			return "", false, fmt.Errorf("%s is empty; remove it to generate a new token", path)
		}
		return token, false, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", false, err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", false, err
	}
	token = hex.EncodeToString(raw)
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		return "", false, fmt.Errorf("write %s: %w", path, err)
	}
	return token, true, nil
}
