package dedup_test

import (
	"context"
	"testing"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/dedup"
	"github.com/stretchr/testify/require"
)

func TestOpenPostgresBackendNil(t *testing.T) {
	idx, err := dedup.Open(config.Config{DedupBackend: "postgres"})
	require.NoError(t, err)
	require.Nil(t, idx)
}

func TestOpenUnknownBackend(t *testing.T) {
	_, err := dedup.Open(config.Config{DedupBackend: "unknown"})
	require.Error(t, err)
}

func TestOpenRocksdbWithoutBuildTag(t *testing.T) {
	_, err := dedup.Open(config.Config{DedupBackend: "rocksdb", RocksDBPath: t.TempDir()})
	require.Error(t, err)
	require.Contains(t, err.Error(), "rocksdb")
}

func TestPutLookupRoundTrip(t *testing.T) {
	idx, err := dedup.Open(config.Config{DedupBackend: "memory", BloomExpectedItems: 100})
	require.NoError(t, err)
	defer idx.Close()

	hash := make([]byte, 32)
	hash[0] = 42
	entry := dedup.Entry{SegmentID: 3, Offset: 100, Size: 200}
	require.NoError(t, idx.Put(hash, entry))
	require.True(t, idx.MightContain(hash))
	got, ok := idx.Lookup(hash)
	require.True(t, ok)
	require.Equal(t, entry, got)
}

func TestRebuildFromPGNilIndex(t *testing.T) {
	require.NoError(t, dedup.RebuildFromPG(context.Background(), nil, &stubRepo{}, 100, 0.001))
}

func TestIndexLen(t *testing.T) {
	idx, err := dedup.Open(config.Config{DedupBackend: "memory"})
	require.NoError(t, err)
	defer idx.Close()

	n, err := idx.Len()
	require.NoError(t, err)
	require.Zero(t, n)

	hash := make([]byte, 32)
	require.NoError(t, idx.Put(hash, dedup.Entry{SegmentID: 0, Offset: 1, Size: 2}))
	n, err = idx.Len()
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestIndexBackendName(t *testing.T) {
	idx, err := dedup.Open(config.Config{DedupBackend: "memory"})
	require.NoError(t, err)
	defer idx.Close()
	require.Equal(t, "memory", idx.Backend())
}
