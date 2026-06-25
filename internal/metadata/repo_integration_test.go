//go:build integration

package metadata_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/stretchr/testify/require"
)

func TestPostgresRepositoryBlobLifecycle(t *testing.T) {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		t.Skip("PG_DSN not set")
	}

	ctx := context.Background()
	if err := metadata.RunMigrations(ctx, dsn, "../../migrations"); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	repo, err := metadata.NewPostgresRepository(ctx, dsn)
	require.NoError(t, err)
	defer repo.Close()

	hash := []byte("integration-test-blob-hash-32b!!")
	_ = repo.WithTx(ctx, func(tx metadata.Tx) error {
		existing, err := tx.GetBlob(ctx, hash)
		require.NoError(t, err)
		if existing != nil {
			return nil
		}
		return tx.InsertBlob(ctx, metadata.ContentBlob{
			ContentHash: hash,
			Size:        10,
			SegmentID:   0,
			Offset:      0,
			RefCount:    1,
			FirstSeenAt: time.Now().UTC(),
		})
	})

	blob, err := repo.GetBlob(ctx, hash)
	require.NoError(t, err)
	require.NotNil(t, blob)

	require.NoError(t, repo.RecordSupplierIngest(ctx, 99, 3, 1, 2))
	stats, err := repo.GetSupplierStats(ctx, 99)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Equal(t, int64(3), stats.TotalRefs)

	blobs, err := repo.ListContentBlobs(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, blobs)
}
