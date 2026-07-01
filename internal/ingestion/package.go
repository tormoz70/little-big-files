package ingestion

import (
	"context"
	"fmt"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/little-big-files/little-big-files/internal/recovery"
	"github.com/little-big-files/little-big-files/internal/storage"
)

const (
	RoleOriginal    = "original"
	RoleMember      = "member"
	RoleUnpackError = "unpack_error"

	StorageSingle          = "single"
	StorageZipWithMembers  = "zip_with_members"
	StorageRawLarge        = "raw_large"

	UnpackOK      = "ok"
	UnpackFailed  = "failed"
	UnpackSkipped = "skipped"

	unpackErrorFilename = "_unpack_error.txt"
)

type Service struct {
	cfg         config.Config
	repo        metadata.Repository
	blobs       *storage.BlobStore
	journal     *recovery.Journal
	unpackQueue *UnpackQueue
}

func NewService(cfg config.Config, repo metadata.Repository, blobs *storage.BlobStore) *Service {
	return &Service{cfg: cfg, repo: repo, blobs: blobs}
}

func (s *Service) SetUnpackQueue(q *UnpackQueue) {
	s.unpackQueue = q
}

func (s *Service) SetJournal(j *recovery.Journal) {
	s.journal = j
}

func (s *Service) journalPackage(pkg *metadata.Package) error {
	if s.journal == nil || pkg == nil {
		return nil
	}
	return s.journal.Append(recovery.EntryFromPackage(pkg))
}

func (s *Service) loadAndJournal(ctx context.Context, packageID int64) (*metadata.Package, error) {
	pkg, err := s.repo.GetPackage(ctx, packageID)
	if err != nil {
		return nil, err
	}
	if err := s.journalPackage(pkg); err != nil {
		return nil, err
	}
	return pkg, nil
}

type ProcessResult struct {
	PackageID int64
}

func (s *Service) ProcessPackage(ctx context.Context, supplierID int, body []byte, filename *string) (*metadata.Package, error) {
	if len(body) == 0 {
		return nil, fmt.Errorf("empty body")
	}
	if int64(len(body)) > s.cfg.MaxBodyBytes {
		return nil, fmt.Errorf("payload too large")
	}

	pkgHash := storage.PackageHash(body)
	payloadType, err := DetectPayload(body)
	if err != nil {
		return nil, err
	}

	canonicalID, err := s.repo.FindCanonicalPackageID(ctx, pkgHash)
	if err != nil {
		return nil, err
	}
	if canonicalID != nil {
		pkg, err := s.clonePackage(ctx, supplierID, pkgHash, payloadType, filename, *canonicalID)
		if err != nil {
			return nil, err
		}
		s.recordIngest(ctx, supplierID, pkg.FileCount, ingestCounters{}, true)
		return pkg, nil
	}

	switch payloadType {
	case PayloadXML:
		return s.ingestXML(ctx, supplierID, pkgHash, body, filename)
	case PayloadZIP:
		return s.ingestZIP(ctx, supplierID, pkgHash, body, filename)
	default:
		return nil, fmt.Errorf("unsupported payload")
	}
}

func (s *Service) clonePackage(ctx context.Context, supplierID int, pkgHash []byte, payloadType PayloadType, filename *string, canonicalID int64) (*metadata.Package, error) {
	canonical, err := s.repo.GetPackage(ctx, canonicalID)
	if err != nil {
		return nil, err
	}
	if canonical == nil {
		return nil, fmt.Errorf("canonical package missing")
	}

	var packageID int64
	err = s.repo.WithTx(ctx, func(tx metadata.Tx) error {
		id, err := tx.CreatePackage(ctx, metadata.CreatePackageInput{
			SupplierID:         supplierID,
			PackageHash:        pkgHash,
			PayloadType:        string(payloadType),
			StorageMode:        canonical.StorageMode,
			OriginalFilename:   filename,
			CanonicalPackageID: &canonicalID,
			FileCount:          canonical.FileCount,
			UnpackError:        canonical.UnpackError,
		})
		if err != nil {
			return err
		}
		packageID = id

		rows := canonical.Files
		var hashes [][]byte
		for _, f := range rows {
			_, err := tx.CreatePackageFile(ctx, packageID, metadata.CreateFileInput{
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
		return tx.IncrementRefCounts(ctx, hashes)
	})
	if err != nil {
		return nil, err
	}
	return s.loadAndJournal(ctx, packageID)
}

func (s *Service) ingestXML(ctx context.Context, supplierID int, pkgHash, body []byte, filename *string) (*metadata.Package, error) {
	var packageID int64
	var counters ingestCounters
	err := s.repo.WithTx(ctx, func(tx metadata.Tx) error {
		hash, created, err := s.blobs.StoreOrRef(ctx, tx, body, storage.RecordXML, supplierID)
		if err != nil {
			return err
		}
		counters.add(created)
		id, err := tx.CreatePackage(ctx, metadata.CreatePackageInput{
			SupplierID:       supplierID,
			PackageHash:      pkgHash,
			PayloadType:      string(PayloadXML),
			StorageMode:      StorageSingle,
			OriginalFilename: filename,
			FileCount:        1,
		})
		if err != nil {
			return err
		}
		packageID = id
		_, err = tx.CreatePackageFile(ctx, packageID, metadata.CreateFileInput{
			BlobHash:         hash,
			Role:             RoleOriginal,
			OriginalFilename: filename,
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	s.recordIngest(ctx, supplierID, 1, counters, false)
	return s.loadAndJournal(ctx, packageID)
}

func (s *Service) ingestZIP(ctx context.Context, supplierID int, pkgHash, body []byte, filename *string) (*metadata.Package, error) {
	large := len(body) > s.cfg.ZipThresholdSize
	if !large {
		count, err := CountZipEntries(body)
		if err == nil && count > s.cfg.ZipThresholdCount {
			large = true
		}
	}

	if large {
		return s.ingestLargeZIP(ctx, supplierID, pkgHash, body, filename)
	}
	return s.ingestSmallZIP(ctx, supplierID, pkgHash, body, filename)
}

func (s *Service) ingestLargeZIP(ctx context.Context, supplierID int, pkgHash, body []byte, filename *string) (*metadata.Package, error) {
	var packageID int64
	var counters ingestCounters
	err := s.repo.WithTx(ctx, func(tx metadata.Tx) error {
		hash, created, err := s.blobs.StoreOrRef(ctx, tx, body, storage.RecordZIP, supplierID)
		if err != nil {
			return err
		}
		counters.add(created)
		id, err := tx.CreatePackage(ctx, metadata.CreatePackageInput{
			SupplierID:       supplierID,
			PackageHash:      pkgHash,
			PayloadType:      string(PayloadZIP),
			StorageMode:      StorageRawLarge,
			OriginalFilename: filename,
			FileCount:        1,
		})
		if err != nil {
			return err
		}
		packageID = id
		_, err = tx.CreatePackageFile(ctx, packageID, metadata.CreateFileInput{
			BlobHash:         hash,
			Role:             RoleOriginal,
			OriginalFilename: filename,
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	s.recordIngest(ctx, supplierID, 1, counters, false)
	if s.cfg.LargeZipAsyncUnpack && s.unpackQueue != nil {
		s.unpackQueue.Enqueue(packageID)
	}
	return s.loadAndJournal(ctx, packageID)
}

func (s *Service) ingestSmallZIP(ctx context.Context, supplierID int, pkgHash, body []byte, filename *string) (*metadata.Package, error) {
	members, unpackErr := UnpackZip(body)
	var unpackErrText *string
	fileCount := 1
	if unpackErr != nil {
		msg := unpackErr.Error()
		unpackErrText = &msg
		fileCount = 2
	} else {
		fileCount = 1 + len(members)
	}

	var packageID int64
	var counters ingestCounters
	err := s.repo.WithTx(ctx, func(tx metadata.Tx) error {
		zipHash, created, err := s.blobs.StoreOrRef(ctx, tx, body, storage.RecordZIP, supplierID)
		if err != nil {
			return err
		}
		counters.add(created)
		id, err := tx.CreatePackage(ctx, metadata.CreatePackageInput{
			SupplierID:       supplierID,
			PackageHash:      pkgHash,
			PayloadType:      string(PayloadZIP),
			StorageMode:      StorageZipWithMembers,
			OriginalFilename: filename,
			FileCount:        fileCount,
			UnpackError:      unpackErrText,
		})
		if err != nil {
			return err
		}
		packageID = id

		_, err = tx.CreatePackageFile(ctx, packageID, metadata.CreateFileInput{
			BlobHash:         zipHash,
			Role:             RoleOriginal,
			OriginalFilename: filename,
		})
		if err != nil {
			return err
		}

		_, _, err = persistZipMembers(ctx, tx, s.blobs, packageID, supplierID, members, unpackErr, &counters)
		return err
	})
	if err != nil {
		return nil, err
	}
	s.recordIngest(ctx, supplierID, fileCount, counters, false)
	return s.loadAndJournal(ctx, packageID)
}
