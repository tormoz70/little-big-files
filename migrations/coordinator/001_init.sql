CREATE TABLE IF NOT EXISTS shard_registry (
    shard_id      SMALLINT PRIMARY KEY,
    state         VARCHAR(10) NOT NULL,
    primary_url   TEXT NOT NULL,
    replica_url   TEXT,
    total_bytes   BIGINT NOT NULL DEFAULT 0,
    sealed_at     TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS global_package_index (
    global_id     BIGINT PRIMARY KEY,
    shard_id      SMALLINT NOT NULL,
    local_id      BIGINT NOT NULL,
    supplier_id   INT NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL,
    package_hash  BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gpi_supplier_time ON global_package_index(supplier_id, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_gpi_shard_local ON global_package_index(shard_id, local_id);

CREATE TABLE IF NOT EXISTS global_xml_index (
    content_hash  BYTEA PRIMARY KEY,
    shard_id      SMALLINT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
