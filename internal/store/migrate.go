package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
)

// Migration files are named NNNN_description.sql. The zero padding keeps
// string order equal to numeric order, which is the order fs.ReadDir returns.
// A file is never edited once it has run somewhere; schema changes are new files.
//
//go:embed migrations/*.sql
var migrations embed.FS

// migrate applies every migration in fsys above the current schema version.
// Each file runs in one transaction together with its schema_version row, so a
// failed file leaves the database exactly as it was and the next start retries it.
func migrate(ctx context.Context, db *sql.DB, fsys fs.FS) error {
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	var current int
	err = db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&current)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	for _, e := range entries {
		version, err := parseVersion(e.Name())
		if err != nil {
			return err
		}
		if version <= current {
			continue
		}
		if err := apply(ctx, db, fsys, e.Name(), version); err != nil {
			return err
		}
	}
	return nil
}

// parseVersion returns the number before the first underscore in "0001_init.sql".
func parseVersion(name string) (int, error) {
	prefix, _, ok := strings.Cut(name, "_")
	if !ok {
		return 0, fmt.Errorf("migrate: %s: no _ after the version number", name)
	}
	v, err := strconv.Atoi(prefix)
	if err != nil {
		return 0, fmt.Errorf("migrate: %s: %w", name, err)
	}
	return v, nil
}

// apply runs one migration file and records its version in a single transaction.
func apply(ctx context.Context, db *sql.DB, fsys fs.FS, name string, version int) error {
	src, err := fs.ReadFile(fsys, name)
	if err != nil {
		return fmt.Errorf("migrate: %s: %w", name, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("migrate: %s: %w", name, err)
	}
	defer func() { _ = tx.Rollback() }() // no-op once Commit has succeeded
	if _, err := tx.ExecContext(ctx, string(src)); err != nil {
		return fmt.Errorf("migrate: %s: %w", name, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_version (version) VALUES (?)`, version); err != nil {
		return fmt.Errorf("migrate: %s: %w", name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("migrate: %s: %w", name, err)
	}
	return nil
}
