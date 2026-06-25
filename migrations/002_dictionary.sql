CREATE TABLE IF NOT EXISTS compression_dictionary (
    id          SERIAL PRIMARY KEY,
    dict_data   BYTEA NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    entry_count INT NOT NULL DEFAULT 0
);
