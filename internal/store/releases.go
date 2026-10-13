package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Release statuses and the moves between them:
//
//	building ──▶ starting ──▶ live ──▶ superseded
//	    │            │
//	    └────────────┴──────▶ failed
//
// A release created without an image is building; MarkBuilt records the
// image and moves it to starting. SetLive moves starting to live and the
// release it replaces to superseded. MarkFailed ends a building or starting
// release. failed and superseded are terminal: a rollback is a new release,
// never an old one revived. Each move is guarded by the current status in
// its UPDATE, so a caller holding a stale view gets ErrInvalidTransition
// instead of rewriting history.
const (
	StatusBuilding   = "building"
	StatusStarting   = "starting"
	StatusLive       = "live"
	StatusFailed     = "failed"
	StatusSuperseded = "superseded"
)

// Release is one immutable (image, config) pair of an app, numbered per app
// from v1. Image is empty while the release is building. ConfigJSON is the
// config vars as a JSON object of strings, frozen when the release is made;
// ManifestJSON is the effective ashore.toml the same way ({} before
// manifests existed, which the manifest package reads as the defaults).
type Release struct {
	ID           int64
	AppID        int64
	Version      int
	Image        string
	GitSHA       string
	ConfigJSON   string
	ManifestJSON string
	Status       string
	Note         string
	CreatedAt    time.Time
}

// Name is how a release is shown: v1, v2, ...
func (r Release) Name() string { return fmt.Sprintf("v%d", r.Version) }

// ParseVersion is the inverse of Release.Name: "v3" is 3.
func ParseVersion(name string) (int, error) {
	n, err := strconv.ParseUint(strings.TrimPrefix(name, "v"), 10, 31)
	if err != nil || !strings.HasPrefix(name, "v") || n < 1 {
		return 0, fmt.Errorf("%q is not a release version (want v1, v2, ...)", name)
	}
	return int(n), nil
}

// NewRelease is what is known about a release when it is created. An empty
// Image means the build has not run yet; the release starts as building.
// An empty ConfigJSON means no config vars; an empty ManifestJSON means
// the defaults.
type NewRelease struct {
	Image        string
	GitSHA       string
	ConfigJSON   string
	ManifestJSON string
	Note         string
}

const releaseColumns = `id, app_id, version, image, COALESCE(git_sha, ''), config_json, manifest_json, status, note, created_at`

func scanRelease(sc scanner) (Release, error) {
	var r Release
	var created int64
	if err := sc.Scan(&r.ID, &r.AppID, &r.Version, &r.Image, &r.GitSHA, &r.ConfigJSON, &r.ManifestJSON, &r.Status, &r.Note, &created); err != nil {
		return Release{}, err
	}
	r.CreatedAt = time.Unix(created, 0).UTC()
	return r, nil
}

// CreateRelease inserts the next release of an app. The version is
// MAX(version)+1 for that app, computed in the same insert; with one
// connection in the pool no two inserts can interleave. An unknown app is
// ErrNotFound.
func (s *Store) CreateRelease(ctx context.Context, appID int64, n NewRelease) (Release, error) {
	if n.ConfigJSON == "" {
		n.ConfigJSON = "{}"
	}
	if !isStringObject(n.ConfigJSON) {
		return Release{}, errors.New("create release: config must be a JSON object of strings")
	}
	if n.ManifestJSON == "" {
		n.ManifestJSON = "{}"
	}
	if !json.Valid([]byte(n.ManifestJSON)) {
		return Release{}, errors.New("create release: manifest must be JSON")
	}
	status := StatusStarting
	if n.Image == "" {
		status = StatusBuilding
	}
	now := time.Now().UTC().Truncate(time.Second)
	r := Release{AppID: appID, Image: n.Image, GitSHA: n.GitSHA, ConfigJSON: n.ConfigJSON, ManifestJSON: n.ManifestJSON, Status: status, Note: n.Note, CreatedAt: now}
	var sha sql.NullString
	if n.GitSHA != "" {
		sha = sql.NullString{String: n.GitSHA, Valid: true}
	}
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO releases (app_id, version, image, git_sha, config_json, manifest_json, status, note, created_at)
		SELECT ?, COALESCE(MAX(version), 0) + 1, ?, ?, ?, ?, ?, ?, ? FROM releases WHERE app_id = ?
		RETURNING id, version`,
		appID, r.Image, sha, r.ConfigJSON, r.ManifestJSON, r.Status, r.Note, now.Unix(), appID).Scan(&r.ID, &r.Version)
	if err != nil {
		if isForeignKey(err) {
			return Release{}, fmt.Errorf("app %d: %w", appID, ErrNotFound)
		}
		return Release{}, fmt.Errorf("create release: %w", err)
	}
	return r, nil
}

// isStringObject reports whether doc is a JSON object whose values are all
// strings. Config is env vars, so that is the only shape a snapshot may
// have; anything else would fail much later, at container start.
func isStringObject(doc string) bool {
	var vars map[string]any
	if err := json.Unmarshal([]byte(doc), &vars); err != nil || vars == nil {
		return false
	}
	for _, v := range vars {
		if _, ok := v.(string); !ok {
			return false
		}
	}
	return true
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

// ReleaseByVersion returns an app's release vN, or ErrNotFound.
func (s *Store) ReleaseByVersion(ctx context.Context, appID int64, version int) (Release, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+releaseColumns+` FROM releases WHERE app_id = ? AND version = ?`, appID, version)
	r, err := scanRelease(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Release{}, fmt.Errorf("release v%d: %w", version, ErrNotFound)
	}
	if err != nil {
		return Release{}, fmt.Errorf("release v%d: %w", version, err)
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

// OpenReleases returns every release still building or starting, across
// all apps. After a daemon restart those are deploys that were cut off; the
// pusher saw the daemon go away and git never moved the ref.
func (s *Store) OpenReleases(ctx context.Context) ([]Release, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+releaseColumns+` FROM releases WHERE status IN (?, ?) ORDER BY id`,
		StatusBuilding, StatusStarting)
	if err != nil {
		return nil, fmt.Errorf("open releases: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Release
	for rows.Next() {
		r, err := scanRelease(rows)
		if err != nil {
			return nil, fmt.Errorf("open releases: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("open releases: %w", err)
	}
	return out, nil
}

// MarkBuilt records the image a building release produced and moves it to
// starting.
func (s *Store) MarkBuilt(ctx context.Context, id int64, image string) error {
	if image == "" {
		return errors.New("mark built: image is required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE releases SET status = ?, image = ? WHERE id = ? AND status = ?`,
		StatusStarting, image, id, StatusBuilding)
	if err != nil {
		return fmt.Errorf("release %d: mark built: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return noChange(ctx, s.db, 0, id, StatusStarting)
	}
	return nil
}

// MarkFailed ends a release that never went live: its build failed, its
// instance never became healthy, or its push was abandoned.
func (s *Store) MarkFailed(ctx context.Context, id int64) error {
	res, err := s.db.ExecContext(ctx, `UPDATE releases SET status = ? WHERE id = ? AND status IN (?, ?)`,
		StatusFailed, id, StatusBuilding, StatusStarting)
	if err != nil {
		return fmt.Errorf("release %d: mark failed: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return noChange(ctx, s.db, 0, id, StatusFailed)
	}
	return nil
}

// SetLive makes a starting release the app's live one and marks the release
// it replaces superseded, in one transaction, so a reader never sees two
// live releases or none.
func (s *Store) SetLive(ctx context.Context, appID, releaseID int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("set live: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `UPDATE releases SET status = ? WHERE id = ? AND app_id = ? AND status = ?`,
		StatusLive, releaseID, appID, StatusStarting)
	if err != nil {
		return fmt.Errorf("set live: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return noChange(ctx, tx, appID, releaseID, StatusLive)
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

// queryer is what *sql.DB and *sql.Tx share, so noChange can look inside a
// transaction that is still open.
type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// noChange explains a status-guarded UPDATE that touched no row: the
// release does not exist (within appID, when it is not 0), or it is in a
// state the move does not start from.
func noChange(ctx context.Context, q queryer, appID, id int64, to string) error {
	var from string
	err := q.QueryRowContext(ctx, `SELECT status FROM releases WHERE id = ? AND (? = 0 OR app_id = ?)`, id, appID, appID).Scan(&from)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("release %d: %w", id, ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("release %d: %w", id, err)
	}
	return fmt.Errorf("release %d: %s -> %s: %w", id, from, to, ErrInvalidTransition)
}
