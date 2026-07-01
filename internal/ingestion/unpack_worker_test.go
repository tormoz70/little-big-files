package ingestion_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/ingestion"
	"github.com/little-big-files/little-big-files/internal/metadata"
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

func TestUnpackQueueRecoversPending(t *testing.T) {
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

	// Ingest a large ZIP but DO NOT enqueue it: simulates a job dropped/lost so
	// the package stays in raw_large state.
	zipBody := makeTestZip(t, []byte(`<?xml version="1.0"?><x/>`))
	ctx := context.Background()
	pkg, err := svc.ProcessPackage(ctx, 1, zipBody, strPtr("z.zip"))
	require.NoError(t, err)
	require.Equal(t, ingestion.StorageRawLarge, pkg.StorageMode)

	pending, err := svc.PendingLargePackageIDs(ctx)
	require.NoError(t, err)
	require.Contains(t, pending, pkg.ID)

	// A fresh queue with recovery enabled must pick the pending package up.
	q := ingestion.NewUnpackQueue(svc, 1, 8)
	svc.SetUnpackQueue(q)
	q.StartRecovery(50 * time.Millisecond)
	defer q.Shutdown()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		pkg, _ = repo.GetPackage(ctx, pkg.ID)
		if pkg.StorageMode == ingestion.StorageZipWithMembers {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.Equal(t, ingestion.StorageZipWithMembers, pkg.StorageMode)
}

func TestUnpackLargePackageRetriesClonePropagationAfterFailure(t *testing.T) {
	baseRepo := testmetadata.NewMemoryRepository()
	repo := &flakyPropagationRepo{MemoryRepository: baseRepo}
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

	zipBody := makeTestZip(t, []byte(`<?xml version="1.0"?><x/>`))
	canonical, err := svc.ProcessPackage(ctx, 1, zipBody, strPtr("z.zip"))
	require.NoError(t, err)
	clone, err := svc.ProcessPackage(ctx, 2, zipBody, strPtr("z2.zip"))
	require.NoError(t, err)
	require.Equal(t, ingestion.StorageRawLarge, clone.StorageMode)

	repo.failCloneID = clone.ID
	err = svc.UnpackLargePackage(ctx, canonical.ID)
	require.Error(t, err)

	cloneAfterFail, err := repo.GetPackage(ctx, clone.ID)
	require.NoError(t, err)
	require.Equal(t, ingestion.StorageRawLarge, cloneAfterFail.StorageMode)

	require.NoError(t, svc.UnpackLargePackage(ctx, canonical.ID))
	cloneAfterRetry, err := repo.GetPackage(ctx, clone.ID)
	require.NoError(t, err)
	require.Equal(t, ingestion.StorageZipWithMembers, cloneAfterRetry.StorageMode)
}

type flakyPropagationRepo struct {
	*testmetadata.MemoryRepository
	failCloneID int64
	failedOnce  bool
}

func (r *flakyPropagationRepo) WithTx(ctx context.Context, fn func(metadata.Tx) error) error {
	return r.MemoryRepository.WithTx(ctx, func(tx metadata.Tx) error {
		return fn(&flakyPropagationTx{Tx: tx, repo: r})
	})
}

type flakyPropagationTx struct {
	metadata.Tx
	repo *flakyPropagationRepo
}

func (tx *flakyPropagationTx) UpdatePackageAfterUnpack(ctx context.Context, packageID int64, storageMode string, fileCount int, unpackError *string) (bool, error) {
	if packageID == tx.repo.failCloneID && !tx.repo.failedOnce {
		tx.repo.failedOnce = true
		return false, fmt.Errorf("injected clone propagation failure")
	}
	return tx.Tx.UpdatePackageAfterUnpack(ctx, packageID, storageMode, fileCount, unpackError)
}
