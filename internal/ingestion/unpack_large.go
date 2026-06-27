package ingestion

import (
	"context"
	"fmt"

	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/little-big-files/little-big-files/internal/storage"
)

// persistZipMembers adds member or unpack_error rows after original already exists.
func persistZipMembers(ctx context.Context, tx metadata.Tx, blobs *storage.BlobStore, packageID int64, supplierID int, members []ZipMember, unpackErr error, counters *ingestCounters) (fileCount int, unpackErrText *string, err error) {
	if unpackErr != nil {
		msg := unpackErr.Error()
		unpackErrText = &msg
		errBytes := []byte(msg)
		errHash, created, err := blobs.StoreOrRef(ctx, tx, errBytes, storage.RecordError, supplierID)
		if err != nil {
			return 0, nil, err
		}
		if counters != nil {
			counters.add(created)
		}
		errName := unpackErrorFilename
		if _, err := tx.CreatePackageFile(ctx, packageID, metadata.CreateFileInput{
			BlobHash:         errHash,
			Role:             RoleUnpackError,
			OriginalFilename: &errName,
		}); err != nil {
			return 0, nil, err
		}
		return 2, unpackErrText, nil
	}

	for i, m := range members {
		memberHash, created, err := blobs.StoreOrRef(ctx, tx, m.Data, storage.RecordXML, supplierID)
		if err != nil {
			return 0, nil, err
		}
		if counters != nil {
			counters.add(created)
		}
		seq := i
		name := m.Filename
		if _, err := tx.CreatePackageFile(ctx, packageID, metadata.CreateFileInput{
			BlobHash:         memberHash,
			Role:             RoleMember,
			OriginalFilename: &name,
			SequenceNumber:   &seq,
		}); err != nil {
			return 0, nil, err
		}
	}
	return 1 + len(members), nil, nil
}

// PendingLargePackageIDs returns packages still in raw_large state, used by the
// unpack queue's recovery scan to re-enqueue work lost across restarts/drops.
func (s *Service) PendingLargePackageIDs(ctx context.Context) ([]int64, error) {
	return s.repo.ListPendingLargePackages(ctx)
}

// UnpackLargePackage unpacks a raw_large package in place and propagates members to early clones.
func (s *Service) UnpackLargePackage(ctx context.Context, packageID int64) error {
	pkg, err := s.repo.GetPackage(ctx, packageID)
	if err != nil {
		return err
	}
	if pkg == nil {
		return fmt.Errorf("package %d not found", packageID)
	}
	if pkg.StorageMode != StorageRawLarge {
		return nil
	}

	orig, err := s.repo.GetOriginalFile(ctx, packageID)
	if err != nil {
		return err
	}
	if orig == nil {
		return fmt.Errorf("package %d has no original", packageID)
	}

	blob, err := s.repo.GetBlob(ctx, orig.BlobHash)
	if err != nil {
		return err
	}
	if blob == nil {
		return fmt.Errorf("blob missing for package %d", packageID)
	}

	zipBytes, err := s.blobs.ReadBlob(*blob)
	if err != nil {
		return err
	}

	members, unpackErr := UnpackZip(zipBytes)

	var fileCount int
	var unpackErrText *string
	err = s.repo.WithTx(ctx, func(tx metadata.Tx) error {
		// Re-check under transaction via fresh read would be ideal; accept rare double-unpack race for v1.
		fc, uet, err := persistZipMembers(ctx, tx, s.blobs, packageID, pkg.SupplierID, members, unpackErr, nil)
		if err != nil {
			return err
		}
		fileCount = fc
		unpackErrText = uet
		return tx.UpdatePackageAfterUnpack(ctx, packageID, StorageZipWithMembers, fileCount, unpackErrText)
	})
	if err != nil {
		return err
	}

	if err := s.propagateUnpackToClones(ctx, packageID); err != nil {
		return err
	}
	_, err = s.loadAndJournal(ctx, packageID)
	return err
}

func (s *Service) propagateUnpackToClones(ctx context.Context, canonicalID int64) error {
	canonical, err := s.repo.GetPackage(ctx, canonicalID)
	if err != nil {
		return err
	}
	if canonical == nil || canonical.StorageMode != StorageZipWithMembers {
		return nil
	}

	cloneIDs, err := s.repo.ListClonePackageIDs(ctx, canonicalID)
	if err != nil {
		return err
	}

	var extraFiles []metadata.PackageFile
	for _, f := range canonical.Files {
		if f.Role == RoleMember || f.Role == RoleUnpackError {
			extraFiles = append(extraFiles, f)
		}
	}

	for _, cloneID := range cloneIDs {
		clone, err := s.repo.GetPackage(ctx, cloneID)
		if err != nil {
			return err
		}
		if clone == nil || clone.StorageMode != StorageRawLarge {
			continue
		}

		err = s.repo.WithTx(ctx, func(tx metadata.Tx) error {
			var hashes [][]byte
			for _, f := range extraFiles {
				_, err := tx.CreatePackageFile(ctx, cloneID, metadata.CreateFileInput{
					BlobHash:         f.BlobHash,
					Role:             f.Role,
					OriginalFilename: f.OriginalFilename,
					SequenceNumber:   f.SequenceNumber,
				})
				if err != nil {
					return err
				}
				hashes = append(hashes, f.BlobHash)
			}
			if err := tx.IncrementRefCounts(ctx, hashes); err != nil {
				return err
			}
			return tx.UpdatePackageAfterUnpack(ctx, cloneID, StorageZipWithMembers, canonical.FileCount, canonical.UnpackError)
		})
		if err != nil {
			return err
		}
	}
	return nil
}
