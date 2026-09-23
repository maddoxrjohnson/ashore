package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"regexp"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrExists      = errors.New("already exists")
	ErrInvalidName = errors.New("invalid app name")
)

// nameRE is the whole rule for app names. A name is a database key, a label in
// the app's hostname, and a path component under the data directory, so it is
// kept deliberately strict.
var nameRE = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

// Store is the daemon's only path to the database. All SQL lives in this package.
type Store struct {
	db *sql.DB
}

// Open opens or creates the SQLite file at path and brings its schema up to date.
func Open(ctx context.Context, path string) (*Store, error) {
	db, err := openDB(ctx, path)
	if err != nil {
		return nil, err
	}
	fsys, err := fs.Sub(migrations, "migrations")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := migrate(ctx, db, fsys); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// openDB connects without touching the schema, so tests can run migrations of
// their own against it.
//
// The pragmas ride in the DSN because *sql.DB is a pool and a PRAGMA statement
// would only apply to whichever connection ran it. The pool is capped at one
// connection: SQLite allows a single writer anyway, and one connection means
// no SQLITE_BUSY between our own goroutines.
func openDB(ctx context.Context, path string) (*sql.DB, error) {
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close() // the ping error is the one worth reporting
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	return db, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

// ValidateName reports whether name is acceptable as an app name.
func ValidateName(name string) error {
	if !nameRE.MatchString(name) {
		return fmt.Errorf("%w: %q", ErrInvalidName, name)
	}
	return nil
}

// isUnique reports whether err is a UNIQUE constraint violation, which is how
// SQLite tells us a name or fingerprint is already taken.
func isUnique(err error) bool {
	var se *sqlite.Error
	return errors.As(err, &se) && se.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE
}
