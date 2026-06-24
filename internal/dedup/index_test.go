package dedup_test

import (
	"context"
	"testing"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/dedup"
	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/stretchr/testify/require"
)

type stubRepo struct {
	blobs []metadata.ContentBlob
}

func (s *stubRepo) ListContentBlobs(ctx context.Context) ([]metadata.ContentBlob, error) {
	return s.blobs, nil
}

func TestBloomMissSkipsLookup(t *testing.T) {
	cfg := config.Config{DedupBackend: "memory", BloomExpectedItems: 1000}
	idx, err := dedup.Open(cfg)
	require.NoError(t, err)
	defer idx.Close()

	hash := make([]byte, 32)
	hash[31] = 0xAB
	require.False(t, idx.MightContain(hash))
	_, ok := idx.Lookup(hash)
	require.False(t, ok)
}

func TestRebuildFromPG(t *testing.T) {
	cfg := config.Config{DedupBackend: "memory", BloomExpectedItems: 1000}
	idx, err := dedup.Open(cfg)
	require.NoError(t, err)
	defer idx.Close()

	hash := make([]byte, 32)
	hash[0] = 1
	repo := &stubRepo{blobs: []metadata.ContentBlob{{
		ContentHash: hash,
		Size:        100,
		SegmentID:   0,
		Offset:      42,
	}}}
	require.NoError(t, dedup.RebuildFromPG(context.Background(), idx, repo, 1000, 0.001))
	require.True(t, idx.MightContain(hash))
	entry, ok := idx.Lookup(hash)
	require.True(t, ok)
	require.Equal(t, 42, int(entry.Offset))
}

func TestBloomFalsePositivePGFallback(t *testing.T) {
	cfg := config.Config{DedupBackend: "memory", BloomExpectedItems: 10, BloomFalsePositive: 0.5}
	idx, err := dedup.Open(cfg)
	require.NoError(t, err)
	defer idx.Close()

	// Seed bloom with unrelated hashes to increase collision chance.
	for i := 0; i < 20; i++ {
		h := make([]byte, 32)
		h[0] = byte(i)
		require.NoError(t, idx.Put(h, dedup.Entry{SegmentID: 0, Offset: int64(i), Size: 1}))
	}

	mystery := make([]byte, 32)
	mystery[0] = 99
	_, ok := idx.Lookup(mystery)
	require.False(t, ok)
}
