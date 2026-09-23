package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Key is one SSH public key allowed to push. Fingerprint is the SHA256 form
// ssh-keygen prints; the SSH server computes it from the presented key and
// looks it up here, so it is the unique column.
type Key struct {
	ID          int64
	Name        string
	Fingerprint string
	PubKey      string
	CreatedAt   time.Time
}

const keyColumns = `id, name, fingerprint, pubkey, created_at`

func scanKey(sc scanner) (Key, error) {
	var k Key
	var created int64
	if err := sc.Scan(&k.ID, &k.Name, &k.Fingerprint, &k.PubKey, &created); err != nil {
		return Key{}, err
	}
	k.CreatedAt = time.Unix(created, 0).UTC()
	return k, nil
}

// AddKey stores a public key. It returns ErrExists if the fingerprint is
// already registered.
func (s *Store) AddKey(ctx context.Context, name, fingerprint, pubkey string) (Key, error) {
	if fingerprint == "" || pubkey == "" {
		return Key{}, errors.New("add key: fingerprint and public key are required")
	}
	now := time.Now().UTC().Truncate(time.Second)
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO ssh_keys (name, fingerprint, pubkey, created_at) VALUES (?, ?, ?, ?)`,
		name, fingerprint, pubkey, now.Unix())
	if err != nil {
		if isUnique(err) {
			return Key{}, fmt.Errorf("key %s: %w", fingerprint, ErrExists)
		}
		return Key{}, fmt.Errorf("add key: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return Key{}, fmt.Errorf("add key: %w", err)
	}
	return Key{ID: id, Name: name, Fingerprint: fingerprint, PubKey: pubkey, CreatedAt: now}, nil
}

// KeyByFingerprint is the authentication lookup. Unknown keys are ErrNotFound.
func (s *Store) KeyByFingerprint(ctx context.Context, fingerprint string) (Key, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+keyColumns+` FROM ssh_keys WHERE fingerprint = ?`, fingerprint)
	k, err := scanKey(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Key{}, fmt.Errorf("key %s: %w", fingerprint, ErrNotFound)
	}
	if err != nil {
		return Key{}, fmt.Errorf("get key: %w", err)
	}
	return k, nil
}

// ListKeys returns every key in the order they were added.
func (s *Store) ListKeys(ctx context.Context) ([]Key, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+keyColumns+` FROM ssh_keys ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list keys: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var keys []Key
	for rows.Next() {
		k, err := scanKey(rows)
		if err != nil {
			return nil, fmt.Errorf("list keys: %w", err)
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list keys: %w", err)
	}
	return keys, nil
}
