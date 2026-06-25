-- Content-addressed storage metadata (Phase 1)

CREATE TABLE IF NOT EXISTS content_blobs (
    content_hash    BYTEA PRIMARY KEY,
    size            INT NOT NULL,
    segment_id      INT NOT NULL,
    "offset"        BIGINT NOT NULL,
    ref_count       BIGINT NOT NULL DEFAULT 1,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS packages (
    id                   BIGSERIAL PRIMARY KEY,
    supplier_id          INT NOT NULL,
    received_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    package_hash         BYTEA NOT NULL,
    payload_type         VARCHAR(10) NOT NULL,
    storage_mode         VARCHAR(20) NOT NULL,
    original_filename    VARCHAR(255),
    canonical_package_id BIGINT REFERENCES packages(id),
    file_count           INT NOT NULL DEFAULT 0,
    unpack_error         TEXT
);

CREATE INDEX IF NOT EXISTS idx_packages_hash ON packages(package_hash);
CREATE INDEX IF NOT EXISTS idx_packages_supplier ON packages(supplier_id, received_at DESC);

CREATE TABLE IF NOT EXISTS package_files (
    id                BIGSERIAL PRIMARY KEY,
    package_id        BIGINT NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
    blob_hash         BYTEA NOT NULL REFERENCES content_blobs(content_hash),
    role              VARCHAR(15) NOT NULL,
    original_filename VARCHAR(255),
    sequence_number   INT
);

CREATE INDEX IF NOT EXISTS idx_package_files_package ON package_files(package_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_package_files_original ON package_files(package_id) WHERE role = 'original';
CREATE UNIQUE INDEX IF NOT EXISTS idx_package_files_unpack_err ON package_files(package_id) WHERE role = 'unpack_error';
