ALTER TABLE shard_registry
    ADD COLUMN IF NOT EXISTS shard_uuid TEXT,
    ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_error TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_shard_registry_uuid
    ON shard_registry (shard_uuid)
    WHERE shard_uuid IS NOT NULL;
