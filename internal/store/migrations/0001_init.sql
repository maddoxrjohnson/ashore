CREATE TABLE apps (
    id          INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL UNIQUE,
    host        TEXT    NOT NULL UNIQUE,
    created_at  INTEGER NOT NULL
);

CREATE TABLE ssh_keys (
    id          INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL,
    fingerprint TEXT    NOT NULL UNIQUE,
    pubkey      TEXT    NOT NULL,
    created_at  INTEGER NOT NULL
);
