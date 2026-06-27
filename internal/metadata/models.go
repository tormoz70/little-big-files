package metadata

import (
	"context"
	"time"
)

type ContentBlob struct {
	ContentHash []byte
	Size        int
	StoredSize  int // on-disk record bytes (header + payload); 0 = unknown/legacy
	SegmentID   int
	Offset      int64
	RefCount    int64
	FirstSeenAt time.Time
}

type PackageFile struct {
	ID               int64
	PackageID        int64
	BlobHash         []byte
	Role             string
	OriginalFilename *string
	SequenceNumber   *int
	Size             int
}

type Package struct {
	ID                 int64
	SupplierID         int
	ReceivedAt         time.Time
	PackageHash        []byte
	PayloadType        string
	StorageMode        string
	OriginalFilename   *string
	CanonicalPackageID *int64
	FileCount          int
	UnpackError        *string
	Files              []PackageFile
}

type CreatePackageInput struct {
	SupplierID         int
	PackageHash        []byte
	PayloadType        string
	StorageMode        string
	OriginalFilename   *string
	CanonicalPackageID *int64
	FileCount          int
	UnpackError        *string
}

type CreateFileInput struct {
	BlobHash         []byte
	Role             string
	OriginalFilename *string
	SequenceNumber   *int
}

type SupplierStats struct {
	SupplierID    int
	TotalPackages int64
	TotalRefs     int64
	DuplicateRefs int64
	LastActivity  time.Time
}

func (s SupplierStats) DedupRatio() float64 {
	if s.TotalRefs == 0 {
		return 0
	}
	return float64(s.DuplicateRefs) / float64(s.TotalRefs)
}

// BlobByteTotals aggregates logical vs on-disk blob sizes for compression metrics.
type BlobByteTotals struct {
	LogicalBytes           int64 // SUM(size) over unique blobs
	StoredBytes            int64 // SUM(stored_size) with fallback to size
	ReferencedLogicalBytes int64 // SUM(size * ref_count)
}

type Tx interface {
	GetBlob(ctx context.Context, hash []byte) (*ContentBlob, error)
	InsertBlob(ctx context.Context, blob ContentBlob) error
	// InsertBlobOrIncrement atomically inserts a new blob or, if a blob with the
	// same content_hash already exists, increments its ref_count. It returns true
	// when a new row was inserted (i.e. the content is genuinely new). This makes
	// concurrent writes of identical new content safe (no unique-violation 500s).
	InsertBlobOrIncrement(ctx context.Context, blob ContentBlob) (bool, error)
	IncrementRefCount(ctx context.Context, hash []byte) error
	IncrementRefCountIfExists(ctx context.Context, hash []byte) (bool, error)
	IncrementRefCounts(ctx context.Context, hashes [][]byte) error
	CreatePackage(ctx context.Context, in CreatePackageInput) (int64, error)
	CreatePackageFile(ctx context.Context, packageID int64, in CreateFileInput) (int64, error)
	GetBlobByHash(ctx context.Context, hash []byte) (*ContentBlob, error)
	UpdatePackageAfterUnpack(ctx context.Context, packageID int64, storageMode string, fileCount int, unpackError *string) error
}

type Repository interface {
	WithTx(ctx context.Context, fn func(Tx) error) error
	FindCanonicalPackageID(ctx context.Context, packageHash []byte) (*int64, error)
	GetPackage(ctx context.Context, id int64) (*Package, error)
	GetPackageFile(ctx context.Context, packageID, fileID int64) (*PackageFile, error)
	GetOriginalFile(ctx context.Context, packageID int64) (*PackageFile, error)
	ClonePackageRefs(ctx context.Context, canonicalID, newPackageID int64) error
	ListPackageFiles(ctx context.Context, packageID int64) ([]PackageFile, error)
	CountContentBlobs(ctx context.Context) (int64, error)
	GetBlob(ctx context.Context, hash []byte) (*ContentBlob, error)
	ListClonePackageIDs(ctx context.Context, canonicalID int64) ([]int64, error)
	ListPendingLargePackages(ctx context.Context) ([]int64, error)
	GetLatestDictionary(ctx context.Context) ([]byte, int, error)
	SaveDictionary(ctx context.Context, dict []byte, entryCount int) error
	ListContentBlobs(ctx context.Context) ([]ContentBlob, error)
	BlobByteTotals(ctx context.Context) (BlobByteTotals, error)
	RecordSupplierIngest(ctx context.Context, supplierID, fileCount, newBlobs, duplicateRefs int) error
	GetSupplierStats(ctx context.Context, supplierID int) (*SupplierStats, error)
}
