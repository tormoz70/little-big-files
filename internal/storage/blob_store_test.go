package storage_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/little-big-files/little-big-files/internal/compress"
	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/dedup"
	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/little-big-files/little-big-files/internal/storage"
	"github.com/stretchr/testify/require"
)

type memTx struct {
	blobs map[string]metadata.ContentBlob
	mu    sync.Mutex
}

func blobKey(h []byte) string { return string(h) }

func newMemTx() *memTx {
	return &memTx{blobs: make(map[string]metadata.ContentBlob)}
}

func (tx *memTx) GetBlob(ctx context.Context, hash []byte) (*metadata.ContentBlob, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	b, ok := tx.blobs[blobKey(hash)]
	if !ok {
		return nil, nil
	}
	cp := b
	return &cp, nil
}

func (tx *memTx) InsertBlob(ctx context.Context, blob metadata.ContentBlob) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.blobs[blobKey(blob.ContentHash)] = blob
	return nil
}

func (tx *memTx) InsertBlobOrIncrement(ctx context.Context, blob metadata.ContentBlob) (bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	k := blobKey(blob.ContentHash)
	if existing, ok := tx.blobs[k]; ok {
		existing.RefCount++
		tx.blobs[k] = existing
		return false, nil
	}
	tx.blobs[k] = blob
	return true, nil
}

func (tx *memTx) IncrementRefCount(ctx context.Context, hash []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	b := tx.blobs[blobKey(hash)]
	b.RefCount++
	tx.blobs[blobKey(hash)] = b
	return nil
}

func (tx *memTx) IncrementRefCounts(ctx context.Context, hashes [][]byte) error {
	for _, h := range hashes {
		if err := tx.IncrementRefCount(ctx, h); err != nil {
			return err
		}
	}
	return nil
}

func (tx *memTx) IncrementRefCountIfExists(ctx context.Context, hash []byte) (bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	b, ok := tx.blobs[blobKey(hash)]
	if !ok {
		return false, nil
	}
	b.RefCount++
	tx.blobs[blobKey(hash)] = b
	return true, nil
}

func (tx *memTx) CreatePackage(ctx context.Context, in metadata.CreatePackageInput) (int64, error) {
	return 0, nil
}
func (tx *memTx) CreatePackageFile(ctx context.Context, packageID int64, in metadata.CreateFileInput) (int64, error) {
	return 0, nil
}
func (tx *memTx) GetBlobByHash(ctx context.Context, hash []byte) (*metadata.ContentBlob, error) {
	return tx.GetBlob(ctx, hash)
}
func (tx *memTx) UpdatePackageAfterUnpack(ctx context.Context, packageID int64, storageMode string, fileCount int, unpackError *string) error {
	return nil
}

func trainingSamples(n int) [][]byte {
	base := `<?xml version="1.0"?><root><item id="%d">value-%d</item></root>`
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, []byte(fmt.Sprintf(base, i, i)))
	}
	return out
}

func TestBlobStoreCompressionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	sm, err := storage.NewSegmentManager(dir, 1024*1024)
	require.NoError(t, err)
	defer sm.Close()

	samples := trainingSamples(200)
	dict, err := compress.TrainDictionary(samples, compress.DefaultDictSize)
	require.NoError(t, err)
	enc, err := compress.NewEncoder(dict, 32)
	require.NoError(t, err)
	defer enc.Close()

	blobs := storage.NewBlobStore(sm, nil, enc, nil)
	tx := newMemTx()
	ctx := context.Background()

	original := samples[0]
	hash, created, err := blobs.StoreOrRef(ctx, tx, original, storage.RecordXML, 1)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, storage.ContentHash(original), hash)

	blob, err := tx.GetBlob(ctx, hash)
	require.NoError(t, err)
	require.NotNil(t, blob)

	readBack, err := blobs.ReadBlob(*blob)
	require.NoError(t, err)
	require.Equal(t, original, readBack)
	require.Greater(t, blob.StoredSize, 0)

	if len(dict) > 0 {
		magic, _, err := sm.ReadRecord(blob.SegmentID, blob.Offset)
		require.NoError(t, err)
		require.True(t, storage.IsCompressedXML(magic))
		require.Less(t, blob.StoredSize, blob.Size)
	}
}

func TestWriteBufferBatchesFlush(t *testing.T) {
	dir := t.TempDir()
	sm, err := storage.NewSegmentManager(dir, 1024*1024)
	require.NoError(t, err)
	defer sm.Close()

	wb := storage.NewWriteBuffer(sm, 256, 50*time.Millisecond)
	sm.SetWriteBuffer(wb)
	defer wb.Close()

	record := storage.EncodeRecord(storage.MagicXML, []byte("payload-one"))
	loc, err := sm.Append(context.Background(), record)
	require.NoError(t, err)

	data, err := sm.Read(loc)
	require.NoError(t, err)
	require.Equal(t, []byte("payload-one"), data)
}

func TestWriteBufferBatchBySize(t *testing.T) {
	dir := t.TempDir()
	sm, err := storage.NewSegmentManager(dir, 1024*1024)
	require.NoError(t, err)
	defer sm.Close()

	wb := storage.NewWriteBuffer(sm, 64, time.Second)
	sm.SetWriteBuffer(wb)
	defer wb.Close()

	record := storage.EncodeRecord(storage.MagicXML, []byte("0123456789012345678901234567890123456789012345678901234567890"))
	for i := 0; i < 3; i++ {
		_, err := sm.Append(context.Background(), record)
		require.NoError(t, err)
	}
}

func TestBlobStoreDedupRef(t *testing.T) {
	dir := t.TempDir()
	sm, err := storage.NewSegmentManager(dir, 1024*1024)
	require.NoError(t, err)
	defer sm.Close()

	cfg := config.Config{DedupBackend: "memory", BloomExpectedItems: 1000}
	idx, err := dedup.Open(cfg)
	require.NoError(t, err)
	defer idx.Close()

	blobs := storage.NewBlobStore(sm, nil, nil, idx)
	tx := newMemTx()
	ctx := context.Background()
	data := []byte(`<?xml version="1.0"?><dup/>`)

	hash1, created1, err := blobs.StoreOrRef(ctx, tx, data, storage.RecordXML, 1)
	require.NoError(t, err)
	require.True(t, created1)

	hash2, created2, err := blobs.StoreOrRef(ctx, tx, data, storage.RecordXML, 1)
	require.NoError(t, err)
	require.False(t, created2)
	require.Equal(t, hash1, hash2)

	blob, err := tx.GetBlob(ctx, hash1)
	require.NoError(t, err)
	require.Equal(t, int64(2), blob.RefCount)
}

func TestBlobStoreWritesSegmentIndex(t *testing.T) {
	dir := t.TempDir()
	sm, err := storage.NewSegmentManager(dir, 1024*1024)
	require.NoError(t, err)
	defer sm.Close()

	idx := storage.NewSegmentIndex(dir)
	defer idx.Close()

	blobs := storage.NewBlobStore(sm, idx, nil, nil)
	tx := newMemTx()
	ctx := context.Background()
	body := []byte(`<?xml version="1.0"?><doc id="1"/>`)

	hash, created, err := blobs.StoreOrRef(ctx, tx, body, storage.RecordXML, 1577)
	require.NoError(t, err)
	require.True(t, created)

	entries, err := storage.ReadIndexFile(filepath.Join(dir, "segment_0000.idx"))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, uint32(1577), entries[0].SupplierID)
	require.Equal(t, storage.MagicXML, entries[0].Magic)
	require.Equal(t, storage.ContentHash(body), hash[:])
}

func TestBlobStoreWritesSegmentIndexCompressedWithDictID(t *testing.T) {
	dir := t.TempDir()
	sm, err := storage.NewSegmentManager(dir, 1024*1024)
	require.NoError(t, err)
	defer sm.Close()

	idx := storage.NewSegmentIndex(dir)
	defer idx.Close()

	samples := trainingSamples(200)
	dict, err := compress.TrainDictionary(samples, compress.DefaultDictSize)
	require.NoError(t, err)
	enc, err := compress.NewEncoder(dict, 32)
	require.NoError(t, err)
	enc.SetDictID(3)
	defer enc.Close()

	blobs := storage.NewBlobStore(sm, idx, enc, nil)
	tx := newMemTx()
	ctx := context.Background()
	original := samples[0]
	for len(original) < 128 {
		original = append(original, original...)
	}

	_, created, err := blobs.StoreOrRef(ctx, tx, original, storage.RecordXML, 2447)
	require.NoError(t, err)
	require.True(t, created)

	entries, err := storage.ReadIndexFile(filepath.Join(dir, "segment_0000.idx"))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, storage.MagicXMLC, entries[0].Magic)
	require.Equal(t, uint32(3), entries[0].DictID)
	require.Equal(t, uint32(2447), entries[0].SupplierID)
}

func TestBlobStoreZIPRecord(t *testing.T) {
	dir := t.TempDir()
	sm, err := storage.NewSegmentManager(dir, 1024*1024)
	require.NoError(t, err)
	defer sm.Close()

	blobs := storage.NewBlobStore(sm, nil, nil, nil)
	tx := newMemTx()
	ctx := context.Background()
	zipData := []byte{0x50, 0x4b, 0x03, 0x04}

	hash, created, err := blobs.StoreOrRef(ctx, tx, zipData, storage.RecordZIP, 1)
	require.NoError(t, err)
	require.True(t, created)

	magic, _, err := sm.ReadRecord(0, 0)
	require.NoError(t, err)
	require.Equal(t, storage.MagicZIP, magic)
	require.NotEmpty(t, hash)
}

func TestPackageHashEqualsContentHash(t *testing.T) {
	data := []byte("same")
	require.Equal(t, storage.ContentHash(data), storage.PackageHash(data))
}
