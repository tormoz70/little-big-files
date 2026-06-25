package ingestion_test

import (
	"context"
	"testing"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/dedup"
	"github.com/little-big-files/little-big-files/internal/ingestion"
	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/little-big-files/little-big-files/internal/storage"
	"github.com/little-big-files/little-big-files/internal/testmetadata"
	"github.com/stretchr/testify/require"
)

func TestDedupStatsOnRepeatedXML(t *testing.T) {
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 64*1024*1024)
	require.NoError(t, err)
	defer segments.Close()

	cfg := config.Config{
		MaxBodyBytes:        16 * 1024 * 1024,
		DedupBackend:        "memory",
		DedupRebuildOnStart: false,
	}
	idx, err := dedup.Open(cfg)
	require.NoError(t, err)
	defer idx.Close()

	blobs := storage.NewBlobStore(segments, nil, nil, idx)
	svc := ingestion.NewService(cfg, repo, blobs)
	ctx := context.Background()

	body := []byte(`<?xml version="1.0"?><doc>same</doc>`)
	_, err = svc.ProcessPackage(ctx, 1, body, nil)
	require.NoError(t, err)
	_, err = svc.ProcessPackage(ctx, 1, body, nil)
	require.NoError(t, err)

	stats, err := repo.GetSupplierStats(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(2), stats.TotalPackages)
	require.Equal(t, int64(2), stats.TotalRefs)
	require.Equal(t, int64(1), stats.DuplicateRefs)
	require.InDelta(t, 0.5, stats.DedupRatio(), 0.01)
}

func TestHotPathSkipsPGOnBloomMiss(t *testing.T) {
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 64*1024*1024)
	require.NoError(t, err)
	defer segments.Close()

	cfg := config.Config{MaxBodyBytes: 16 * 1024 * 1024, DedupBackend: "memory", DedupRebuildOnStart: false}
	idx, err := dedup.Open(cfg)
	require.NoError(t, err)
	defer idx.Close()

	var pgLookups int
	wrapped := &countingRepo{MemoryRepository: repo, onGetBlob: func() { pgLookups++ }}
	blobs := storage.NewBlobStore(segments, nil, nil, idx)
	svc := ingestion.NewService(cfg, wrapped, blobs)
	ctx := context.Background()

	body := []byte(`<?xml version="1.0"?><doc>brand-new</doc>`)
	_, err = svc.ProcessPackage(ctx, 7, body, nil)
	require.NoError(t, err)
	require.Zero(t, pgLookups, "bloom miss should not call GetBlob before insert")
}

type countingRepo struct {
	*testmetadata.MemoryRepository
	onGetBlob func()
}

func (c *countingRepo) WithTx(ctx context.Context, fn func(metadata.Tx) error) error {
	return c.MemoryRepository.WithTx(ctx, func(tx metadata.Tx) error {
		return fn(&countingTx{Tx: tx, onGetBlob: c.onGetBlob})
	})
}

type countingTx struct {
	metadata.Tx
	onGetBlob func()
}

func (t *countingTx) GetBlob(ctx context.Context, hash []byte) (*metadata.ContentBlob, error) {
	if t.onGetBlob != nil {
		t.onGetBlob()
	}
	return t.Tx.GetBlob(ctx, hash)
}

// satisfy import
var _ metadata.Repository = (*countingRepo)(nil)
