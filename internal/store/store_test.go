package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"testing/fstest"
)

// open returns a migrated store on a fresh temp file, closed when the test ends.
func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

// openRaw returns a connection with no schema, for driving migrate directly.
func openRaw(t *testing.T) *sql.DB {
	t.Helper()
	db, err := openDB(context.Background(), filepath.Join(t.TempDir(), "raw.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func schemaVersion(t *testing.T, db *sql.DB) int {
	t.Helper()
	var v int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var n int
	err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n == 1
}

func TestOpenMigratesFromEmpty(t *testing.T) {
	s := open(t)
	if v := schemaVersion(t, s.db); v != 1 {
		t.Fatalf("schema version = %d, want 1", v)
	}
	for _, table := range []string{"apps", "ssh_keys"} {
		if !tableExists(t, s.db, table) {
			t.Errorf("table %s missing after migration", table)
		}
	}
	var mode string
	if err := s.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Errorf("journal_mode = %q, want wal", mode)
	}
}

func TestOpenTwiceIsIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	var rows int
	if err := s.db.QueryRow(`SELECT count(*) FROM schema_version`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("schema_version has %d rows, want 1: a migration ran twice", rows)
	}
	if v := schemaVersion(t, s.db); v != 1 {
		t.Errorf("schema version = %d, want 1", v)
	}
}

func TestOpenMissingDirectory(t *testing.T) {
	_, err := Open(context.Background(), filepath.Join(t.TempDir(), "missing", "test.db"))
	if err == nil {
		t.Fatal("Open succeeded although the parent directory does not exist")
	}
}

func TestMigrateAppliesOnlyNewFiles(t *testing.T) {
	ctx := context.Background()
	db := openRaw(t)
	first := fstest.MapFS{
		"0001_a.sql": {Data: []byte(`CREATE TABLE a (x INTEGER);`)},
	}
	if err := migrate(ctx, db, first); err != nil {
		t.Fatal(err)
	}
	// If 0001 ran again, CREATE TABLE a would fail with "table exists".
	both := fstest.MapFS{
		"0001_a.sql": first["0001_a.sql"],
		"0002_b.sql": {Data: []byte(`CREATE TABLE b (y INTEGER);`)},
	}
	if err := migrate(ctx, db, both); err != nil {
		t.Fatal(err)
	}
	if v := schemaVersion(t, db); v != 2 {
		t.Errorf("schema version = %d, want 2", v)
	}
	if !tableExists(t, db, "a") || !tableExists(t, db, "b") {
		t.Error("expected tables a and b")
	}
	if err := migrate(ctx, db, both); err != nil {
		t.Fatalf("third run was not a no-op: %v", err)
	}
}

func TestMigrateRollsBackFailedFile(t *testing.T) {
	ctx := context.Background()
	db := openRaw(t)
	fsys := fstest.MapFS{
		"0001_a.sql": {Data: []byte(`CREATE TABLE a (x INTEGER);`)},
		// The second statement fails, so the first must be undone with it.
		"0002_bad.sql": {Data: []byte(`CREATE TABLE b (y INTEGER); CREATE TABLE b (y INTEGER);`)},
	}
	if err := migrate(ctx, db, fsys); err == nil {
		t.Fatal("migrate succeeded with a broken file")
	}
	if v := schemaVersion(t, db); v != 1 {
		t.Errorf("schema version = %d, want 1", v)
	}
	if tableExists(t, db, "b") {
		t.Error("table b exists after its migration was rolled back")
	}
}

func TestMigrateRejectsBadFilename(t *testing.T) {
	db := openRaw(t)
	for _, name := range []string{"init.sql", "x_init.sql"} {
		fsys := fstest.MapFS{name: {Data: []byte(`SELECT 1;`)}}
		if err := migrate(context.Background(), db, fsys); err == nil {
			t.Errorf("%s: accepted as a migration name", name)
		}
	}
}
