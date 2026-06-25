package metadata

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(ctx context.Context, dsn string) (*PostgresRepository, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &PostgresRepository{pool: pool}, nil
}

func (r *PostgresRepository) Close() {
	r.pool.Close()
}

func RunMigrations(ctx context.Context, dsn, migrationsDir string) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		sql, err := os.ReadFile(filepath.Join(migrationsDir, e.Name()))
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", e.Name(), err)
		}
	}
	return nil
}

func (r *PostgresRepository) WithTx(ctx context.Context, fn func(Tx) error) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(&pgTx{tx: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type pgTx struct {
	tx pgx.Tx
}

func (t *pgTx) GetBlob(ctx context.Context, hash []byte) (*ContentBlob, error) {
	row := t.tx.QueryRow(ctx, `
		SELECT content_hash, size, stored_size, segment_id, "offset", ref_count, first_seen_at
		FROM content_blobs WHERE content_hash = $1`, hash)
	var b ContentBlob
	err := row.Scan(&b.ContentHash, &b.Size, &b.StoredSize, &b.SegmentID, &b.Offset, &b.RefCount, &b.FirstSeenAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func (t *pgTx) GetBlobByHash(ctx context.Context, hash []byte) (*ContentBlob, error) {
	return t.GetBlob(ctx, hash)
}

func (t *pgTx) InsertBlob(ctx context.Context, blob ContentBlob) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO content_blobs (content_hash, size, stored_size, segment_id, "offset", ref_count, first_seen_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		blob.ContentHash, blob.Size, blob.StoredSize, blob.SegmentID, blob.Offset, blob.RefCount, blob.FirstSeenAt)
	return err
}

func (t *pgTx) IncrementRefCount(ctx context.Context, hash []byte) error {
	_, err := t.tx.Exec(ctx, `UPDATE content_blobs SET ref_count = ref_count + 1 WHERE content_hash = $1`, hash)
	return err
}

func (t *pgTx) IncrementRefCountIfExists(ctx context.Context, hash []byte) (bool, error) {
	tag, err := t.tx.Exec(ctx, `UPDATE content_blobs SET ref_count = ref_count + 1 WHERE content_hash = $1`, hash)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (t *pgTx) IncrementRefCounts(ctx context.Context, hashes [][]byte) error {
	for _, h := range hashes {
		if err := t.IncrementRefCount(ctx, h); err != nil {
			return err
		}
	}
	return nil
}

func (t *pgTx) CreatePackage(ctx context.Context, in CreatePackageInput) (int64, error) {
	var id int64
	err := t.tx.QueryRow(ctx, `
		INSERT INTO packages (
			supplier_id, package_hash, payload_type, storage_mode,
			original_filename, canonical_package_id, file_count, unpack_error
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id`,
		in.SupplierID, in.PackageHash, in.PayloadType, in.StorageMode,
		in.OriginalFilename, in.CanonicalPackageID, in.FileCount, in.UnpackError,
	).Scan(&id)
	return id, err
}

func (t *pgTx) CreatePackageFile(ctx context.Context, packageID int64, in CreateFileInput) (int64, error) {
	var id int64
	err := t.tx.QueryRow(ctx, `
		INSERT INTO package_files (package_id, blob_hash, role, original_filename, sequence_number)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id`,
		packageID, in.BlobHash, in.Role, in.OriginalFilename, in.SequenceNumber,
	).Scan(&id)
	return id, err
}

func (t *pgTx) UpdatePackageAfterUnpack(ctx context.Context, packageID int64, storageMode string, fileCount int, unpackError *string) error {
	_, err := t.tx.Exec(ctx, `
		UPDATE packages SET storage_mode = $2, file_count = $3, unpack_error = $4
		WHERE id = $1 AND storage_mode = 'raw_large'`,
		packageID, storageMode, fileCount, unpackError)
	return err
}

func (r *PostgresRepository) FindCanonicalPackageID(ctx context.Context, packageHash []byte) (*int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx, `
		SELECT id FROM packages WHERE package_hash = $1 ORDER BY id LIMIT 1`, packageHash).Scan(&id)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func (r *PostgresRepository) ListClonePackageIDs(ctx context.Context, canonicalID int64) ([]int64, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id FROM packages WHERE canonical_package_id = $1 ORDER BY id`, canonicalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *PostgresRepository) ClonePackageRefs(ctx context.Context, canonicalID, newPackageID int64) error {
	return r.WithTx(ctx, func(tx Tx) error {
		var unpackErr *string
		err := r.pool.QueryRow(ctx, `SELECT unpack_error FROM packages WHERE id = $1`, canonicalID).Scan(&unpackErr)
		if err != nil && err != pgx.ErrNoRows {
			return err
		}
		if err == nil && unpackErr != nil {
			_, err = r.pool.Exec(ctx, `UPDATE packages SET unpack_error = $2 WHERE id = $1`, newPackageID, unpackErr)
			if err != nil {
				return err
			}
		}

		rows, err := r.pool.Query(ctx, `
			SELECT blob_hash, role, original_filename, sequence_number
			FROM package_files WHERE package_id = $1 ORDER BY id`, canonicalID)
		if err != nil {
			return err
		}
		defer rows.Close()

		var hashes [][]byte
		for rows.Next() {
			var in CreateFileInput
			if err := rows.Scan(&in.BlobHash, &in.Role, &in.OriginalFilename, &in.SequenceNumber); err != nil {
				return err
			}
			if _, err := tx.CreatePackageFile(ctx, newPackageID, in); err != nil {
				return err
			}
			hashes = append(hashes, in.BlobHash)
		}
		return tx.IncrementRefCounts(ctx, hashes)
	})
}

func (r *PostgresRepository) GetPackage(ctx context.Context, id int64) (*Package, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, supplier_id, received_at, package_hash, payload_type, storage_mode,
		       original_filename, canonical_package_id, file_count, unpack_error
		FROM packages WHERE id = $1`, id)
	var p Package
	err := row.Scan(
		&p.ID, &p.SupplierID, &p.ReceivedAt, &p.PackageHash, &p.PayloadType, &p.StorageMode,
		&p.OriginalFilename, &p.CanonicalPackageID, &p.FileCount, &p.UnpackError,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	files, err := r.ListPackageFiles(ctx, id)
	if err != nil {
		return nil, err
	}
	p.Files = files
	return &p, nil
}

func (r *PostgresRepository) ListPackageFiles(ctx context.Context, packageID int64) ([]PackageFile, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT pf.id, pf.package_id, pf.blob_hash, pf.role, pf.original_filename,
		       pf.sequence_number, cb.size
		FROM package_files pf
		JOIN content_blobs cb ON cb.content_hash = pf.blob_hash
		WHERE pf.package_id = $1
		ORDER BY pf.id`, packageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []PackageFile
	for rows.Next() {
		var f PackageFile
		if err := rows.Scan(
			&f.ID, &f.PackageID, &f.BlobHash, &f.Role, &f.OriginalFilename,
			&f.SequenceNumber, &f.Size,
		); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, rows.Err()
}

func (r *PostgresRepository) GetPackageFile(ctx context.Context, packageID, fileID int64) (*PackageFile, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT pf.id, pf.package_id, pf.blob_hash, pf.role, pf.original_filename,
		       pf.sequence_number, cb.size
		FROM package_files pf
		JOIN content_blobs cb ON cb.content_hash = pf.blob_hash
		WHERE pf.package_id = $1 AND pf.id = $2`, packageID, fileID)
	var f PackageFile
	err := row.Scan(
		&f.ID, &f.PackageID, &f.BlobHash, &f.Role, &f.OriginalFilename,
		&f.SequenceNumber, &f.Size,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &f, nil
}

func (r *PostgresRepository) GetOriginalFile(ctx context.Context, packageID int64) (*PackageFile, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT pf.id, pf.package_id, pf.blob_hash, pf.role, pf.original_filename,
		       pf.sequence_number, cb.size
		FROM package_files pf
		JOIN content_blobs cb ON cb.content_hash = pf.blob_hash
		WHERE pf.package_id = $1 AND pf.role = 'original'`, packageID)
	var f PackageFile
	err := row.Scan(
		&f.ID, &f.PackageID, &f.BlobHash, &f.Role, &f.OriginalFilename,
		&f.SequenceNumber, &f.Size,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &f, nil
}

func (r *PostgresRepository) CountContentBlobs(ctx context.Context) (int64, error) {
	var n int64
	err := r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM content_blobs`).Scan(&n)
	return n, err
}

func (r *PostgresRepository) GetBlob(ctx context.Context, hash []byte) (*ContentBlob, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT content_hash, size, stored_size, segment_id, "offset", ref_count, first_seen_at
		FROM content_blobs WHERE content_hash = $1`, hash)
	var b ContentBlob
	err := row.Scan(&b.ContentHash, &b.Size, &b.StoredSize, &b.SegmentID, &b.Offset, &b.RefCount, &b.FirstSeenAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func (r *PostgresRepository) GetLatestDictionary(ctx context.Context) ([]byte, int, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT dict_data, entry_count FROM compression_dictionary
		ORDER BY id DESC LIMIT 1`)
	var data []byte
	var count int
	err := row.Scan(&data, &count)
	if err == pgx.ErrNoRows {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	return data, count, nil
}

func (r *PostgresRepository) SaveDictionary(ctx context.Context, dict []byte, entryCount int) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO compression_dictionary (dict_data, entry_count) VALUES ($1, $2)`,
		dict, entryCount)
	return err
}

func (r *PostgresRepository) ListContentBlobs(ctx context.Context) ([]ContentBlob, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT content_hash, size, stored_size, segment_id, "offset", ref_count, first_seen_at
		FROM content_blobs ORDER BY first_seen_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ContentBlob
	for rows.Next() {
		var b ContentBlob
		if err := rows.Scan(&b.ContentHash, &b.Size, &b.StoredSize, &b.SegmentID, &b.Offset, &b.RefCount, &b.FirstSeenAt); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}


func (r *PostgresRepository) UpsertContentBlob(ctx context.Context, blob ContentBlob) error {
	stored := blob.StoredSize
	if stored == 0 {
		stored = blob.Size
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO content_blobs (content_hash, size, stored_size, segment_id, "offset", ref_count, first_seen_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (content_hash) DO UPDATE SET
			size = EXCLUDED.size,
			stored_size = EXCLUDED.stored_size,
			segment_id = EXCLUDED.segment_id,
			"offset" = EXCLUDED."offset"`,
		blob.ContentHash, blob.Size, stored, blob.SegmentID, blob.Offset, blob.RefCount, blob.FirstSeenAt)
	return err
}

func (r *PostgresRepository) RestorePackage(ctx context.Context, p Package) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO packages (
			id, supplier_id, received_at, package_hash, payload_type, storage_mode,
			original_filename, canonical_package_id, file_count, unpack_error
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (id) DO UPDATE SET
			supplier_id = EXCLUDED.supplier_id,
			received_at = EXCLUDED.received_at,
			package_hash = EXCLUDED.package_hash,
			payload_type = EXCLUDED.payload_type,
			storage_mode = EXCLUDED.storage_mode,
			original_filename = EXCLUDED.original_filename,
			canonical_package_id = EXCLUDED.canonical_package_id,
			file_count = EXCLUDED.file_count,
			unpack_error = EXCLUDED.unpack_error`,
		p.ID, p.SupplierID, p.ReceivedAt, p.PackageHash, p.PayloadType, p.StorageMode,
		p.OriginalFilename, p.CanonicalPackageID, p.FileCount, p.UnpackError)
	return err
}

func (r *PostgresRepository) RestorePackageFile(ctx context.Context, f PackageFile) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO package_files (id, package_id, blob_hash, role, original_filename, sequence_number)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (id) DO UPDATE SET
			package_id = EXCLUDED.package_id,
			blob_hash = EXCLUDED.blob_hash,
			role = EXCLUDED.role,
			original_filename = EXCLUDED.original_filename,
			sequence_number = EXCLUDED.sequence_number`,
		f.ID, f.PackageID, f.BlobHash, f.Role, f.OriginalFilename, f.SequenceNumber)
	return err
}

func (r *PostgresRepository) TruncateRecoveryTables(ctx context.Context) error {
	_, err := r.pool.Exec(ctx, `TRUNCATE package_files, packages, content_blobs, supplier_stats RESTART IDENTITY CASCADE`)
	return err
}

func (r *PostgresRepository) RecomputeRefCounts(ctx context.Context) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE content_blobs b SET ref_count = COALESCE(x.cnt, 0)
		FROM (
			SELECT blob_hash, COUNT(*)::bigint AS cnt FROM package_files GROUP BY blob_hash
		) x
		WHERE b.content_hash = x.blob_hash`)
	return err
}

func (r *PostgresRepository) ResetIDSequences(ctx context.Context) error {
	stmts := []string{
		`SELECT setval(pg_get_serial_sequence('packages','id'), COALESCE((SELECT MAX(id) FROM packages), 1))`,
		`SELECT setval(pg_get_serial_sequence('package_files','id'), COALESCE((SELECT MAX(id) FROM package_files), 1))`,
	}
	for _, s := range stmts {
		if _, err := r.pool.Exec(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

func (r *PostgresRepository) BlobByteTotals(ctx context.Context) (BlobByteTotals, error) {
	var totals BlobByteTotals
	err := r.pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(size), 0),
			COALESCE(SUM(CASE WHEN stored_size > 0 THEN stored_size ELSE size END), 0),
			COALESCE(SUM(size * ref_count), 0)
		FROM content_blobs`).Scan(
		&totals.LogicalBytes,
		&totals.StoredBytes,
		&totals.ReferencedLogicalBytes,
	)
	return totals, err
}

func (r *PostgresRepository) RecordSupplierIngest(ctx context.Context, supplierID, fileCount, newBlobs, duplicateRefs int) error {
	dup := duplicateRefs
	if dup < 0 {
		dup = 0
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO supplier_stats (supplier_id, total_packages, total_refs, duplicate_refs, last_activity)
		VALUES ($1, 1, $2, $3, NOW())
		ON CONFLICT (supplier_id) DO UPDATE SET
			total_packages = supplier_stats.total_packages + 1,
			total_refs = supplier_stats.total_refs + EXCLUDED.total_refs,
			duplicate_refs = supplier_stats.duplicate_refs + EXCLUDED.duplicate_refs,
			last_activity = NOW()`,
		supplierID, fileCount, dup)
	return err
}

func (r *PostgresRepository) GetSupplierStats(ctx context.Context, supplierID int) (*SupplierStats, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT supplier_id, total_packages, total_refs, duplicate_refs, last_activity
		FROM supplier_stats WHERE supplier_id = $1`, supplierID)
	var s SupplierStats
	err := row.Scan(&s.SupplierID, &s.TotalPackages, &s.TotalRefs, &s.DuplicateRefs, &s.LastActivity)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}
