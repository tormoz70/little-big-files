CREATE UNIQUE INDEX IF NOT EXISTS idx_shard_registry_single_active
    ON shard_registry ((1))
    WHERE state = 'active';
