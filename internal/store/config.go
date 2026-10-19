package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
)

var (
	ErrInvalidKey  = errors.New("invalid config key")
	ErrReservedKey = errors.New("reserved config key")
)

// keyRE is the portable shell rule for an environment variable name, which
// is what a config var becomes inside the container.
var keyRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

const maxKeyLen = 256

// ValidateKey reports whether key can be a config var. PORT is refused
// because the runtime sets it, and a value here would silently lose.
func ValidateKey(key string) error {
	if len(key) > maxKeyLen || !keyRE.MatchString(key) {
		return fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}
	if key == "PORT" {
		return fmt.Errorf("%w: PORT is set by the runtime", ErrReservedKey)
	}
	return nil
}

// GetConfig returns an app's config vars. An app with none, or an unknown
// app, gets an empty map; callers look the app up first.
func (s *Store) GetConfig(ctx context.Context, appID int64) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key, value FROM config WHERE app_id = ?`, appID)
	if err != nil {
		return nil, fmt.Errorf("get config: %w", err)
	}
	defer func() { _ = rows.Close() }()
	vars := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("get config: %w", err)
		}
		vars[k] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get config: %w", err)
	}
	return vars, nil
}

// UpdateConfig applies every set and unset in one transaction, so a
// release created afterwards snapshots all of the change or none of it. It
// reports whether anything is different from before: setting a key to the
// value it already has, or unsetting one that is not there, is not a
// change, and the caller can skip the redeploy. An unknown app is
// ErrNotFound; every key is validated before anything is written.
func (s *Store) UpdateConfig(ctx context.Context, appID int64, set map[string]string, unset []string) (changed bool, err error) {
	for k := range set {
		if err := ValidateKey(k); err != nil {
			return false, err
		}
	}
	for _, k := range unset {
		if err := ValidateKey(k); err != nil {
			return false, err
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("update config: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Sorted, so two callers with the same change run the same statements.
	for _, k := range slices.Sorted(maps.Keys(set)) {
		res, err := tx.ExecContext(ctx, `INSERT INTO config (app_id, key, value) VALUES (?, ?, ?)
			ON CONFLICT (app_id, key) DO UPDATE SET value = excluded.value WHERE value <> excluded.value`,
			appID, k, set[k])
		if err != nil {
			if isForeignKey(err) {
				return false, fmt.Errorf("app %d: %w", appID, ErrNotFound)
			}
			return false, fmt.Errorf("update config: %w", err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			changed = true
		}
	}
	for _, k := range unset {
		res, err := tx.ExecContext(ctx, `DELETE FROM config WHERE app_id = ? AND key = ?`, appID, k)
		if err != nil {
			return false, fmt.Errorf("update config: %w", err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			changed = true
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("update config: %w", err)
	}
	return changed, nil
}

// MarshalConfig is the snapshot a release stores: one JSON object, keys
// sorted, so equal configs have equal snapshots.
func MarshalConfig(vars map[string]string) string {
	if len(vars) == 0 {
		return "{}"
	}
	b, _ := json.Marshal(vars) // a map of strings cannot fail to encode
	return string(b)
}

// UnmarshalConfig is the inverse, for turning a release's snapshot back
// into the environment it ran with.
func UnmarshalConfig(doc string) (map[string]string, error) {
	vars := make(map[string]string)
	if doc == "" {
		return vars, nil
	}
	if !isStringObject(doc) {
		return nil, errors.New("config snapshot is not a JSON object of strings")
	}
	if err := json.Unmarshal([]byte(doc), &vars); err != nil {
		return nil, fmt.Errorf("config snapshot: %w", err)
	}
	return vars, nil
}
