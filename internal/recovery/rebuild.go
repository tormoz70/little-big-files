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
