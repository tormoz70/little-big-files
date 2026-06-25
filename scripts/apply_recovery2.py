#!/usr/bin/env python3
import os, pathlib
ROOT = pathlib.Path(__file__).resolve().parents[1]

def w(rel, content):
    p = ROOT / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content.lstrip('\n'), encoding='utf-8', newline='\n')
    print('wrote', rel)

def patch(rel, old, new, count=1):
    p = ROOT / rel
    text = p.read_text(encoding='utf-8')
    if old not in text:
        raise SystemExit(f'patch miss {rel}: {old[:100]!r}')
    p.write_text(text.replace(old, new, count), encoding='utf-8', newline='\n')
    print('patched', rel)

w('internal/recovery/rebuild.go', r'''
package recovery

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/little-big-files/little-big-files/internal/compress"
	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/little-big-files/little-big-files/internal/storage"
)

type RebuildOptions struct {
	DataDir  string
	DataRoot string
	Journal  string
	Apply    bool
}

func Rebuild(ctx context.Context, repo *metadata.PostgresRepository, opt RebuildOptions) error {
	if opt.DataRoot == "" {
		opt.DataRoot = filepath.Dir(opt.DataDir)
	}
	if opt.Journal == "" {
		opt.Journal = filepath.Join(opt.DataDir, JournalFile)
	}

	if opt.Apply {
		if err := repo.TruncateRecoveryTables(ctx); err != nil {
			return err
		}
	}

	sidecar := compress.NewSidecar(opt.DataRoot)
	if dictID, dict, err := sidecar.LoadCurrent(); err != nil {
		return err
	} else if len(dict) > 0 && opt.Apply {
		if err := repo.SaveDictionary(ctx, dict, 0); err != nil {
			return err
		}
		_ = dictID
	}

	idxPaths, err := storage.ListIndexFiles(opt.DataDir)
	if err != nil {
		return err
	}
	blobCount := 0
	for _, path := range idxPaths {
		var segID int
		base := filepath.Base(path)
		if _, err := fmt.Sscanf(base, "segment_%04d.idx", &segID); err != nil {
			continue
		}
		entries, err := storage.ReadIndexFile(path)
		if err != nil {
			return err
		}
		for _, e := range entries {
			blob := metadata.ContentBlob{
				ContentHash: e.Hash[:],
				Size:        int(e.LogicalSize),
				StoredSize:  int(e.StoredSize),
				SegmentID:   segID,
				Offset:      e.Offset,
				RefCount:    1,
				FirstSeenAt: time.Now().UTC(),
			}
			blobCount++
			if opt.Apply {
				if err := repo.UpsertContentBlob(ctx, blob); err != nil {
					return err
				}
			}
		}
	}

	entries, err := ReadJournal(opt.Journal)
	if err != nil {
		return err
	}
	for _, je := range entries {
		if opt.Apply {
			if err := restoreJournalEntry(ctx, repo, je); err != nil {
				return err
			}
		}
	}
	if opt.Apply {
		if err := repo.RecomputeRefCounts(ctx); err != nil {
			return err
		}
		if err := repo.ResetIDSequences(ctx); err != nil {
			return err
		}
	}
	fmt.Printf("recovery: blobs=%d journal_entries=%d apply=%v\n", blobCount, len(entries), opt.Apply)
	return nil
}

func restoreJournalEntry(ctx context.Context, repo *metadata.PostgresRepository, je JournalEntry) error {
	pkgHash, err := hex.DecodeString(je.PackageHash)
	if err != nil {
		return fmt.Errorf("package %d hash: %w", je.PackageID, err)
	}
	receivedAt, err := time.Parse(time.RFC3339Nano, je.ReceivedAt)
	if err != nil {
		receivedAt, err = time.Parse(time.RFC3339, je.ReceivedAt)
		if err != nil {
			return fmt.Errorf("package %d received_at: %w", je.PackageID, err)
		}
	}
	if err := repo.RestorePackage(ctx, metadata.Package{
		ID:                 je.PackageID,
		SupplierID:         je.SupplierID,
		ReceivedAt:         receivedAt,
		PackageHash:        pkgHash,
		PayloadType:        je.PayloadType,
		StorageMode:        je.StorageMode,
		OriginalFilename:   je.OriginalFilename,
		CanonicalPackageID: je.CanonicalPackageID,
		FileCount:          je.FileCount,
		UnpackError:        je.UnpackError,
	}); err != nil {
		return err
	}
	for _, f := range je.Files {
		bh, err := hex.DecodeString(f.BlobHash)
		if err != nil {
			return err
		}
		if err := repo.RestorePackageFile(ctx, metadata.PackageFile{
			ID:               f.FileID,
			PackageID:        je.PackageID,
			BlobHash:         bh,
			Role:             f.Role,
			OriginalFilename: f.OriginalFilename,
			SequenceNumber:   f.SequenceNumber,
		}); err != nil {
			return err
		}
	}
	return nil
}

func DataRootFromDataDir(dataDir string) string {
	return filepath.Dir(dataDir)
}

func EnsureJournalPath(dataDir string) string {
	return filepath.Join(dataDir, JournalFile)
}

func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
''')

w('cmd/recovery-tool/main.go', r'''
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/dedup"
	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/little-big-files/little-big-files/internal/recovery"
)

func main() {
	apply := flag.Bool("apply", false, "write rebuilt metadata to PostgreSQL")
	flag.Parse()
	cfg := config.Load()
	ctx := context.Background()

	repo, err := metadata.NewPostgresRepository(ctx, cfg.PGDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer repo.Close()

	opt := recovery.RebuildOptions{
		DataDir:  cfg.DataDir,
		DataRoot: recovery.DataRootFromDataDir(cfg.DataDir),
		Apply:    *apply,
	}
	if err := recovery.Rebuild(ctx, repo, opt); err != nil {
		log.Fatal(err)
	}
	if *apply {
		idx, err := dedup.Open(cfg)
		if err != nil {
			log.Fatal(err)
		}
		if idx != nil {
			defer idx.Close()
			if err := dedup.RebuildFromPG(ctx, idx, repo, cfg.BloomExpectedItems, cfg.BloomFalsePositive); err != nil {
				log.Fatal(err)
			}
		}
		fmt.Println("dedup index rebuilt")
	}
}
''')

# metadata recovery methods
patch('internal/metadata/repo.go', '''func (r *PostgresRepository) BlobByteTotals(ctx context.Context) (BlobByteTotals, error) {''', r'''
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

func (r *PostgresRepository) BlobByteTotals(ctx context.Context) (BlobByteTotals, error) {''')

print('phase2 done')
