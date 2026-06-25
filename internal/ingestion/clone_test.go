package ingestion_test

import (
	"context"
	"testing"
	"time"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/ingestion"
	"github.com/little-big-files/little-big-files/internal/storage"
	"github.com/little-big-files/little-big-files/internal/testmetadata"
	"github.com/stretchr/testify/require"
)

func TestLargeZipCloneGetsMembersAfterUnpack(t *testing.T) {
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 64*1024*1024)
	require.NoError(t, err)
	defer segments.Close()

	cfg := config.Config{
		MaxBodyBytes:        16 * 1024 * 1024,
		ZipThresholdSize:    10,
		LargeZipAsyncUnpack: false,
	}
	blobs := storage.NewBlobStore(segments, nil, nil, nil)
	svc := ingestion.NewService(cfg, repo, blobs)
	ctx := context.Background()

	xml := []byte(`<?xml version="1.0"?><shared/>`)
	zipBody := makeTestZip(t, xml)

	canonical, err := svc.ProcessPackage(ctx, 1, zipBody, strPtr("orig.zip"))
	require.NoError(t, err)
	require.Equal(t, ingestion.StorageRawLarge, canonical.StorageMode)

	clone, err := svc.ProcessPackage(ctx, 2, zipBody, strPtr("clone.zip"))
	require.NoError(t, err)
	require.Equal(t, ingestion.StorageRawLarge, clone.StorageMode)

	require.NoError(t, svc.UnpackLargePackage(ctx, canonical.ID))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		clone, err = repo.GetPackage(ctx, clone.ID)
		require.NoError(t, err)
		if clone.StorageMode == ingestion.StorageZipWithMembers {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, ingestion.StorageZipWithMembers, clone.StorageMode)
	require.GreaterOrEqual(t, len(clone.Files), 2)
}

func TestClonePackagePreservesFileCount(t *testing.T) {
	svc, repo, _ := newIngestSvc(t, config.Config{MaxBodyBytes: 16 * 1024 * 1024})
	ctx := context.Background()
	body := []byte(`<?xml version="1.0"?><x/>`)

	first, err := svc.ProcessPackage(ctx, 1, body, strPtr("a.xml"))
	require.NoError(t, err)

	second, err := svc.ProcessPackage(ctx, 2, body, strPtr("b.xml"))
	require.NoError(t, err)
	require.Equal(t, first.FileCount, second.FileCount)
	require.NotEqual(t, first.ID, second.ID)

	stats, err := repo.GetSupplierStats(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.DuplicateRefs)
}
