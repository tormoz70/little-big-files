package ingestion_test

import (
	"archive/zip"
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/ingestion"
	"github.com/little-big-files/little-big-files/internal/storage"
	"github.com/little-big-files/little-big-files/internal/testmetadata"
	"github.com/stretchr/testify/require"
)

func TestLargeZipAsyncUnpack(t *testing.T) {
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 64*1024*1024)
	require.NoError(t, err)
	defer segments.Close()

	cfg := config.Config{
		MaxBodyBytes:        16 * 1024 * 1024,
		ZipThresholdSize:    100, // force large path for small test zip
		ZipThresholdCount:     100,
		LargeZipAsyncUnpack: true,
	}
	blobs := storage.NewBlobStore(segments, nil, nil)
	svc := ingestion.NewService(cfg, repo, blobs)
	q := ingestion.NewUnpackQueue(svc, 1, 8)
	svc.SetUnpackQueue(q)
	defer q.Shutdown()

	xml := []byte(`<?xml version="1.0"?><seans></seans>`)
	zipBody := makeTestZip(t, xml)

	ctx := context.Background()
	pkg, err := svc.ProcessPackage(ctx, 1, zipBody, strPtr("big.zip"))
	require.NoError(t, err)
	require.Equal(t, ingestion.StorageRawLarge, pkg.StorageMode)
	require.Len(t, pkg.Files, 1)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		pkg, err = repo.GetPackage(ctx, pkg.ID)
		require.NoError(t, err)
		if pkg.StorageMode == ingestion.StorageZipWithMembers {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.Equal(t, ingestion.StorageZipWithMembers, pkg.StorageMode)
	require.GreaterOrEqual(t, len(pkg.Files), 2)
}

func makeTestZip(t *testing.T, xml []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, err := w.Create("a.xml")
	require.NoError(t, err)
	_, err = f.Write(xml)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func strPtr(s string) *string { return &s }
