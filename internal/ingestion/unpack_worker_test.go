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

func TestUnpackQueueShutdown(t *testing.T) {
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 64*1024*1024)
	require.NoError(t, err)
	defer segments.Close()

	cfg := config.Config{
		MaxBodyBytes:        16 * 1024 * 1024,
		ZipThresholdSize:    10,
		LargeZipAsyncUnpack: true,
	}
	blobs := storage.NewBlobStore(segments, nil, nil, nil)
	svc := ingestion.NewService(cfg, repo, blobs)
	q := ingestion.NewUnpackQueue(svc, 0, 0) // defaults to 1 worker, 64 queue
	svc.SetUnpackQueue(q)

	zipBody := makeTestZip(t, []byte(`<?xml version="1.0"?><x/>`))
	ctx := context.Background()
	pkg, err := svc.ProcessPackage(ctx, 1, zipBody, strPtr("z.zip"))
	require.NoError(t, err)
	q.Enqueue(pkg.ID)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pkg, _ = repo.GetPackage(ctx, pkg.ID)
		if pkg.StorageMode == ingestion.StorageZipWithMembers {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	q.Shutdown()
	pkg, err = repo.GetPackage(ctx, pkg.ID)
	require.NoError(t, err)
	require.Equal(t, ingestion.StorageZipWithMembers, pkg.StorageMode)
}

func TestUnpackLargePackageIdempotent(t *testing.T) {
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 64*1024*1024)
	require.NoError(t, err)
	defer segments.Close()

	cfg := config.Config{MaxBodyBytes: 16 * 1024 * 1024, ZipThresholdSize: 10}
	blobs := storage.NewBlobStore(segments, nil, nil, nil)
	svc := ingestion.NewService(cfg, repo, blobs)
	ctx := context.Background()

	zipBody := makeTestZip(t, []byte(`<?xml version="1.0"?><x/>`))
	pkg, err := svc.ProcessPackage(ctx, 1, zipBody, strPtr("z.zip"))
	require.NoError(t, err)

	require.NoError(t, svc.UnpackLargePackage(ctx, pkg.ID))
	require.NoError(t, svc.UnpackLargePackage(ctx, pkg.ID)) // already unpacked

	updated, err := repo.GetPackage(ctx, pkg.ID)
	require.NoError(t, err)
	require.Equal(t, ingestion.StorageZipWithMembers, updated.StorageMode)
}

func TestUnpackLargePackageNotFound(t *testing.T) {
	svc, _, _ := newIngestSvc(t, config.Config{MaxBodyBytes: 1024})
	err := svc.UnpackLargePackage(context.Background(), 9999)
	require.Error(t, err)
}
