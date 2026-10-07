CREATE TABLE node_metadata (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    network_id TEXT NOT NULL,
    genesis_hash BYTEA NOT NULL CHECK (octet_length(genesis_hash) = 32),
    db_pp_blue_score BIGINT NOT NULL CHECK (db_pp_blue_score >= 0)
);

CREATE TABLE administrative_metadata (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    last_reinitialization_token TEXT
);

INSERT INTO administrative_metadata (singleton, last_reinitialization_token)
VALUES (TRUE, NULL);
