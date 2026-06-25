CREATE TABLE IF NOT EXISTS supplier_stats (
    supplier_id     INT PRIMARY KEY,
    total_packages  BIGINT NOT NULL DEFAULT 0,
    total_refs      BIGINT NOT NULL DEFAULT 0,
    duplicate_refs  BIGINT NOT NULL DEFAULT 0,
    last_activity   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
