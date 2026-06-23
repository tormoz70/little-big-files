package metadata

import (
	"context"
	"time"
)

type ContentBlob struct {
	ContentHash []byte
	Size        int
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

type Tx interface {
	GetBlob(ctx context.Context, hash []byte) (*ContentBlob, error)
	InsertBlob(ctx context.Context, blob ContentBlob) error
	IncrementRefCount(ctx context.Context, hash []byte) error
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
	GetLatestDictionary(ctx context.Context) ([]byte, int, error)
	SaveDictionary(ctx context.Context, dict []byte, entryCount int) error
}
