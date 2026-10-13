CREATE TABLE config (
    app_id  INTEGER NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
    key     TEXT    NOT NULL,
    value   TEXT    NOT NULL,
    PRIMARY KEY (app_id, key)
);
