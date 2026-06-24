package testmetadata

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/little-big-files/little-big-files/internal/metadata"
)

// MemoryRepository is an in-memory metadata store for tests.
type MemoryRepository struct {
	mu      sync.Mutex
	blobs   map[string]metadata.ContentBlob
	pkgs    map[int64]metadata.Package
	files   map[int64]metadata.PackageFile
	stats   map[int]metadata.SupplierStats
	nextPkg int64
	nextFile int64
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		blobs:    make(map[string]metadata.ContentBlob),
		pkgs:     make(map[int64]metadata.Package),
		files:    make(map[int64]metadata.PackageFile),
		stats:    make(map[int]metadata.SupplierStats),
		nextPkg:  1,
		nextFile: 1,
	}
}

func key(b []byte) string { return string(b) }

type memTx struct {
	repo *MemoryRepository
}

func (r *MemoryRepository) WithTx(ctx context.Context, fn func(metadata.Tx) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fn(&memTx{repo: r})
}

func (t *memTx) GetBlobByHash(ctx context.Context, hash []byte) (*metadata.ContentBlob, error) {
	return t.GetBlob(ctx, hash)
}

func (t *memTx) GetBlob(ctx context.Context, hash []byte) (*metadata.ContentBlob, error) {
	b, ok := t.repo.blobs[key(hash)]
	if !ok {
		return nil, nil
	}
	cp := b
	return &cp, nil
}

func (t *memTx) InsertBlob(ctx context.Context, blob metadata.ContentBlob) error {
	t.repo.blobs[key(blob.ContentHash)] = blob
	return nil
}

func (t *memTx) IncrementRefCount(ctx context.Context, hash []byte) error {
	k := key(hash)
	b, ok := t.repo.blobs[k]
	if !ok {
		return fmt.Errorf("blob not found")
	}
	b.RefCount++
	t.repo.blobs[k] = b
	return nil
}

func (t *memTx) IncrementRefCountIfExists(ctx context.Context, hash []byte) (bool, error) {
	k := key(hash)
	b, ok := t.repo.blobs[k]
	if !ok {
		return false, nil
	}
	b.RefCount++
	t.repo.blobs[k] = b
	return true, nil
}

func (t *memTx) IncrementRefCounts(ctx context.Context, hashes [][]byte) error {
	for _, h := range hashes {
		if err := t.IncrementRefCount(ctx, h); err != nil {
			return err
		}
	}
	return nil
}

func (t *memTx) CreatePackage(ctx context.Context, in metadata.CreatePackageInput) (int64, error) {
	id := t.repo.nextPkg
	t.repo.nextPkg++
	p := metadata.Package{
		ID:                 id,
		SupplierID:         in.SupplierID,
		ReceivedAt:         time.Now().UTC(),
		PackageHash:        append([]byte(nil), in.PackageHash...),
		PayloadType:        in.PayloadType,
		StorageMode:        in.StorageMode,
		OriginalFilename:   in.OriginalFilename,
		CanonicalPackageID: in.CanonicalPackageID,
		FileCount:          in.FileCount,
		UnpackError:        in.UnpackError,
	}
	t.repo.pkgs[id] = p
	return id, nil
}

func (t *memTx) CreatePackageFile(ctx context.Context, packageID int64, in metadata.CreateFileInput) (int64, error) {
	id := t.repo.nextFile
	t.repo.nextFile++
	blob, ok := t.repo.blobs[key(in.BlobHash)]
	if !ok {
		return 0, fmt.Errorf("blob missing")
	}
	f := metadata.PackageFile{
		ID:               id,
		PackageID:        packageID,
		BlobHash:         append([]byte(nil), in.BlobHash...),
		Role:             in.Role,
		OriginalFilename: in.OriginalFilename,
		SequenceNumber:   in.SequenceNumber,
		Size:             blob.Size,
	}
	t.repo.files[id] = f
	p := t.repo.pkgs[packageID]
	p.Files = append(p.Files, f)
	t.repo.pkgs[packageID] = p
	return id, nil
}

func (t *memTx) UpdatePackageAfterUnpack(ctx context.Context, packageID int64, storageMode string, fileCount int, unpackError *string) error {
	p, ok := t.repo.pkgs[packageID]
	if !ok || p.StorageMode != "raw_large" {
		return nil
	}
	p.StorageMode = storageMode
	p.FileCount = fileCount
	p.UnpackError = unpackError
	t.repo.pkgs[packageID] = p
	return nil
}

func (r *MemoryRepository) FindCanonicalPackageID(ctx context.Context, packageHash []byte) (*int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var minID *int64
	for id, p := range r.pkgs {
		if string(p.PackageHash) == string(packageHash) && p.CanonicalPackageID == nil {
			if minID == nil || id < *minID {
				v := id
				minID = &v
			}
		}
	}
	return minID, nil
}

func (r *MemoryRepository) ListClonePackageIDs(ctx context.Context, canonicalID int64) ([]int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var ids []int64
	for id, p := range r.pkgs {
		if p.CanonicalPackageID != nil && *p.CanonicalPackageID == canonicalID {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func (r *MemoryRepository) GetPackage(ctx context.Context, id int64) (*metadata.Package, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.pkgs[id]
	if !ok {
		return nil, nil
	}
	cp := p
	cp.Files = listFiles(r.files, id)
	return &cp, nil
}

func listFiles(m map[int64]metadata.PackageFile, packageID int64) []metadata.PackageFile {
	var out []metadata.PackageFile
	for _, f := range m {
		if f.PackageID == packageID {
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *MemoryRepository) ListPackageFiles(ctx context.Context, packageID int64) ([]metadata.PackageFile, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return listFiles(r.files, packageID), nil
}

func (r *MemoryRepository) GetPackageFile(ctx context.Context, packageID, fileID int64) (*metadata.PackageFile, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, ok := r.files[fileID]
	if !ok || f.PackageID != packageID {
		return nil, nil
	}
	return &f, nil
}

func (r *MemoryRepository) GetOriginalFile(ctx context.Context, packageID int64) (*metadata.PackageFile, error) {
	files, err := r.ListPackageFiles(ctx, packageID)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f.Role == "original" {
			cp := f
			return &cp, nil
		}
	}
	return nil, nil
}

func (r *MemoryRepository) ClonePackageRefs(ctx context.Context, canonicalID, newPackageID int64) error {
	return fmt.Errorf("not used")
}

func (r *MemoryRepository) CountContentBlobs(ctx context.Context) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return int64(len(r.blobs)), nil
}

func (r *MemoryRepository) GetBlob(ctx context.Context, hash []byte) (*metadata.ContentBlob, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.blobs[key(hash)]
	if !ok {
		return nil, nil
	}
	cp := b
	return &cp, nil
}

func (r *MemoryRepository) GetLatestDictionary(ctx context.Context) ([]byte, int, error) {
	return nil, 0, nil
}

func (r *MemoryRepository) SaveDictionary(ctx context.Context, dict []byte, entryCount int) error {
	return nil
}

func (r *MemoryRepository) ListContentBlobs(ctx context.Context) ([]metadata.ContentBlob, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]metadata.ContentBlob, 0, len(r.blobs))
	for _, b := range r.blobs {
		out = append(out, b)
	}
	return out, nil
}

func (r *MemoryRepository) RecordSupplierIngest(ctx context.Context, supplierID, fileCount, newBlobs, duplicateRefs int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.stats[supplierID]
	s.SupplierID = supplierID
	s.TotalPackages++
	s.TotalRefs += int64(fileCount)
	s.DuplicateRefs += int64(duplicateRefs)
	s.LastActivity = time.Now().UTC()
	r.stats[supplierID] = s
	return nil
}

func (r *MemoryRepository) GetSupplierStats(ctx context.Context, supplierID int) (*metadata.SupplierStats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.stats[supplierID]
	if !ok {
		return nil, nil
	}
	cp := s
	return &cp, nil
}

// PostgresAdapter wraps MemoryRepository for API server expecting *PostgresRepository.
// Tests use setupHandlerEnv instead.
