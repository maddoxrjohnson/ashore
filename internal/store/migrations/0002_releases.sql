CREATE TABLE releases (
    id          INTEGER PRIMARY KEY,
    app_id      INTEGER NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
    version     INTEGER NOT NULL,
    image       TEXT    NOT NULL,
    git_sha     TEXT,
    config_json TEXT    NOT NULL DEFAULT '{}',
    status      TEXT    NOT NULL CHECK (status IN ('building', 'starting', 'live', 'failed', 'superseded')),
    note        TEXT    NOT NULL DEFAULT '',
    created_at  INTEGER NOT NULL,
    UNIQUE (app_id, version)
);

ALTER TABLE apps ADD COLUMN live_release_id INTEGER REFERENCES releases(id);
