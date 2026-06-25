//go:build integration

package recovery_test

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/little-big-files/little-big-files/internal/compress"
	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/little-big-files/little-big-files/internal/recovery"
	"github.com/little-big-files/little-big-files/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestRebuildApplyRestoresPackageAndBlob(t *testing.T) {
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

	dataRoot := t.TempDir()
	dataDir := filepath.Join(dataRoot, "segments")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))

	blobBody := []byte(`<?xml version="1.0"?><recovered/>`)
	hash := storage.ContentHash(blobBody)
	record := storage.EncodeRecord(storage.MagicXML, blobBody)
	segPath := filepath.Join(dataDir, "segment_0000.dat")
	require.NoError(t, os.WriteFile(segPath, record, 0o644))

	idx := storage.NewSegmentIndex(dataDir)
	var h [32]byte
	copy(h[:], hash)
	require.NoError(t, idx.Append(0, storage.IndexEntry{
		Offset: 0, StoredSize: uint32(len(record)), LogicalSize: uint32(len(blobBody)),
		Magic: storage.MagicXML, Hash: h, SupplierID: 1577,
	}))
	idx.Close()

	fn := "recovered.xml"
	j, err := recovery.NewJournal(dataDir)
	require.NoError(t, err)
	require.NoError(t, j.Append(recovery.JournalEntry{
		PackageID:   1001,
		SupplierID:  1577,
		ReceivedAt:  recovery.FormatTimeUTC(time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC)),
		PackageHash: hex.EncodeToString(storage.PackageHash(blobBody)),
		PayloadType: "xml",
		StorageMode: "single",
		FileCount:   1,
		OriginalFilename: &fn,
		Files: []recovery.FileRef{
			{FileID: 2001, BlobHash: hex.EncodeToString(hash), Role: "original", OriginalFilename: &fn},
		},
	}))
	j.Close()

	require.NoError(t, compress.NewSidecar(dataRoot).Save(1, []byte("integration-dict")))

	require.NoError(t, recovery.Rebuild(ctx, repo, recovery.RebuildOptions{
		DataDir:  dataDir,
		DataRoot: dataRoot,
		Apply:    true,
	}))

	blob, err := repo.GetBlob(ctx, hash)
	require.NoError(t, err)
	require.NotNil(t, blob)
	require.Equal(t, 0, blob.SegmentID)
	require.Equal(t, int64(0), blob.Offset)

	pkg, err := repo.GetPackage(ctx, 1001)
	require.NoError(t, err)
	require.NotNil(t, pkg)
	require.Equal(t, 1577, pkg.SupplierID)
	require.Len(t, pkg.Files, 1)
	require.Equal(t, int64(2001), pkg.Files[0].ID)
	require.Equal(t, int64(1), blob.RefCount)
}
