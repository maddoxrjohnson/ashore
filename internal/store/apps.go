package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// App is one deployable application. Host is the hostname the router matches;
// the caller derives it from the name and the configured domain.
type App struct {
	ID        int64
	Name      string
	Host      string
	CreatedAt time.Time
}

// scanner is what *sql.Row and *sql.Rows have in common, so one scan function
// serves both the single-row and the list queries.
type scanner interface {
	Scan(dest ...any) error
}

const appColumns = `id, name, host, created_at`

func scanApp(sc scanner) (App, error) {
	var a App
	var created int64
	if err := sc.Scan(&a.ID, &a.Name, &a.Host, &created); err != nil {
		return App{}, err
	}
	a.CreatedAt = time.Unix(created, 0).UTC()
	return a, nil
}

// CreateApp inserts a new app. It returns ErrInvalidName for a bad name and
// ErrExists when the name or host is already taken.
func (s *Store) CreateApp(ctx context.Context, name, host string) (App, error) {
	if err := ValidateName(name); err != nil {
		return App{}, err
	}
	// Seconds are the unit stored, so truncate up front and the value handed
	// back equals the value read back later.
	now := time.Now().UTC().Truncate(time.Second)
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO apps (name, host, created_at) VALUES (?, ?, ?)`, name, host, now.Unix())
	if err != nil {
		if isUnique(err) {
			return App{}, fmt.Errorf("app %s: %w", name, ErrExists)
		}
		return App{}, fmt.Errorf("create app %s: %w", name, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return App{}, fmt.Errorf("create app %s: %w", name, err)
	}
	return App{ID: id, Name: name, Host: host, CreatedAt: now}, nil
}

// GetApp returns the app with that name, or ErrNotFound.
func (s *Store) GetApp(ctx context.Context, name string) (App, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+appColumns+` FROM apps WHERE name = ?`, name)
	a, err := scanApp(row)
	if errors.Is(err, sql.ErrNoRows) {
		return App{}, fmt.Errorf("app %s: %w", name, ErrNotFound)
	}
	if err != nil {
		return App{}, fmt.Errorf("get app %s: %w", name, err)
	}
	return a, nil
}

// ListApps returns every app ordered by name.
func (s *Store) ListApps(ctx context.Context) ([]App, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+appColumns+` FROM apps ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list apps: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var apps []App
	for rows.Next() {
		a, err := scanApp(rows)
		if err != nil {
			return nil, fmt.Errorf("list apps: %w", err)
		}
		apps = append(apps, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list apps: %w", err)
	}
	return apps, nil
}

// DeleteApp removes the app row, or returns ErrNotFound if there is none.
func (s *Store) DeleteApp(ctx context.Context, name string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM apps WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("delete app %s: %w", name, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete app %s: %w", name, err)
	}
	if n == 0 {
		return fmt.Errorf("app %s: %w", name, ErrNotFound)
	}
	return nil
}
