package storage_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/little-big-files/little-big-files/internal/compress"
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

	blobs := storage.NewBlobStore(sm, enc, nil)
	tx := newMemTx()
	ctx := context.Background()

	original := samples[0]
	hash, created, err := blobs.StoreOrRef(ctx, tx, original, storage.RecordXML)
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, storage.ContentHash(original), hash)

	blob, err := tx.GetBlob(ctx, hash)
	require.NoError(t, err)
	require.NotNil(t, blob)

	readBack, err := blobs.ReadBlob(*blob)
	require.NoError(t, err)
	require.Equal(t, original, readBack)

	if len(dict) > 0 {
		magic, _, err := sm.ReadRecord(blob.SegmentID, blob.Offset)
		require.NoError(t, err)
		require.True(t, storage.IsCompressedXML(magic))
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
	loc, err := sm.Append(record)
	require.NoError(t, err)

	data, err := sm.Read(loc)
	require.NoError(t, err)
	require.Equal(t, []byte("payload-one"), data)
}
