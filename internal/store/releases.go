package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Release statuses. A release is created starting (its image exists, its
// instance does not yet), becomes live when the router points at it, and
// ends failed (never became healthy) or superseded (a later release went
// live). building is reserved for a release row created before its build.
const (
	StatusBuilding   = "building"
	StatusStarting   = "starting"
	StatusLive       = "live"
	StatusFailed     = "failed"
	StatusSuperseded = "superseded"
)

// Release is one immutable (image, config) pair of an app, numbered per app
// from v1. ConfigJSON is the config snapshot; empty until Phase 2 fills it.
type Release struct {
	ID         int64
	AppID      int64
	Version    int
	Image      string
	GitSHA     string
	ConfigJSON string
	Status     string
	Note       string
	CreatedAt  time.Time
}

// Name is how a release is shown: v1, v2, ...
func (r Release) Name() string { return fmt.Sprintf("v%d", r.Version) }

const releaseColumns = `id, app_id, version, image, COALESCE(git_sha, ''), config_json, status, note, created_at`

func scanRelease(sc scanner) (Release, error) {
	var r Release
	var created int64
	if err := sc.Scan(&r.ID, &r.AppID, &r.Version, &r.Image, &r.GitSHA, &r.ConfigJSON, &r.Status, &r.Note, &created); err != nil {
		return Release{}, err
	}
	r.CreatedAt = time.Unix(created, 0).UTC()
	return r, nil
}

// CreateRelease inserts the next release of an app, in status starting.
// The version is MAX(version)+1 for that app, computed in the same insert;
// with one connection in the pool no two inserts can interleave. An unknown
// app is ErrNotFound.
func (s *Store) CreateRelease(ctx context.Context, appID int64, image, gitSHA, note string) (Release, error) {
	if image == "" {
		return Release{}, errors.New("create release: image is required")
	}
	now := time.Now().UTC().Truncate(time.Second)
	r := Release{AppID: appID, Image: image, GitSHA: gitSHA, ConfigJSON: "{}", Status: StatusStarting, Note: note, CreatedAt: now}
	var sha sql.NullString
	if gitSHA != "" {
		sha = sql.NullString{String: gitSHA, Valid: true}
	}
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO releases (app_id, version, image, git_sha, config_json, status, note, created_at)
		SELECT ?, COALESCE(MAX(version), 0) + 1, ?, ?, ?, ?, ?, ? FROM releases WHERE app_id = ?
		RETURNING id, version`,
		appID, image, sha, r.ConfigJSON, r.Status, note, now.Unix(), appID).Scan(&r.ID, &r.Version)
	if err != nil {
		if isForeignKey(err) {
			return Release{}, fmt.Errorf("app %d: %w", appID, ErrNotFound)
		}
		return Release{}, fmt.Errorf("create release: %w", err)
	}
	return r, nil
}

// GetRelease returns one release by id, or ErrNotFound.
func (s *Store) GetRelease(ctx context.Context, id int64) (Release, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+releaseColumns+` FROM releases WHERE id = ?`, id)
	r, err := scanRelease(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Release{}, fmt.Errorf("release %d: %w", id, ErrNotFound)
	}
	if err != nil {
		return Release{}, fmt.Errorf("get release %d: %w", id, err)
	}
	return r, nil
}

// LiveRelease returns the app's live release, or ErrNotFound when nothing
// has gone live yet.
func (s *Store) LiveRelease(ctx context.Context, appID int64) (Release, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+releaseColumns+` FROM releases
		WHERE id = (SELECT live_release_id FROM apps WHERE id = ?)`, appID)
	r, err := scanRelease(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Release{}, fmt.Errorf("live release of app %d: %w", appID, ErrNotFound)
	}
	if err != nil {
		return Release{}, fmt.Errorf("live release of app %d: %w", appID, err)
	}
	return r, nil
}

// ListReleases returns an app's releases, oldest first.
func (s *Store) ListReleases(ctx context.Context, appID int64) ([]Release, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+releaseColumns+` FROM releases WHERE app_id = ? ORDER BY version`, appID)
	if err != nil {
		return nil, fmt.Errorf("list releases: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Release
	for rows.Next() {
		r, err := scanRelease(rows)
		if err != nil {
			return nil, fmt.Errorf("list releases: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list releases: %w", err)
	}
	return out, nil
}

// SetReleaseStatus records a transition. An unknown id is ErrNotFound; a
// status outside the known set is refused by the schema.
func (s *Store) SetReleaseStatus(ctx context.Context, id int64, status string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE releases SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return fmt.Errorf("release %d: set status %s: %w", id, status, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("release %d: %w", id, ErrNotFound)
	}
	return nil
}

// SetLive makes releaseID the app's live release and marks the one it
// replaces superseded, in one transaction, so a reader never sees two live
// releases or none.
func (s *Store) SetLive(ctx context.Context, appID, releaseID int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("set live: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `UPDATE releases SET status = ? WHERE id = ? AND app_id = ?`, StatusLive, releaseID, appID)
	if err != nil {
		return fmt.Errorf("set live: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("release %d of app %d: %w", releaseID, appID, ErrNotFound)
	}
	_, err = tx.ExecContext(ctx, `UPDATE releases SET status = ? WHERE app_id = ? AND status = ? AND id <> ?`,
		StatusSuperseded, appID, StatusLive, releaseID)
	if err != nil {
		return fmt.Errorf("set live: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE apps SET live_release_id = ? WHERE id = ?`, releaseID, appID); err != nil {
		return fmt.Errorf("set live: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("set live: %w", err)
	}
	return nil
}
