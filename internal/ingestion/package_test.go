package ingestion_test

import (
	"archive/zip"
	"bytes"
	"context"
	"testing"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/dedup"
	"github.com/little-big-files/little-big-files/internal/ingestion"
	"github.com/little-big-files/little-big-files/internal/storage"
	"github.com/little-big-files/little-big-files/internal/testmetadata"
	"github.com/stretchr/testify/require"
)

func newIngestSvc(t *testing.T, cfg config.Config) (*ingestion.Service, *testmetadata.MemoryRepository, *storage.SegmentManager) {
	t.Helper()
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 64*1024*1024)
	require.NoError(t, err)
	t.Cleanup(func() { _ = segments.Close() })

	idx, err := dedup.Open(config.Config{DedupBackend: "memory", BloomExpectedItems: 10000})
	require.NoError(t, err)
	t.Cleanup(func() { _ = idx.Close() })

	blobs := storage.NewBlobStore(segments, nil, nil, idx)
	return ingestion.NewService(cfg, repo, blobs), repo, segments
}

func TestProcessPackageEmptyBody(t *testing.T) {
	svc, _, _ := newIngestSvc(t, config.Config{MaxBodyBytes: 1024})
	_, err := svc.ProcessPackage(context.Background(), 1, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty")
}

func TestProcessPackageTooLarge(t *testing.T) {
	svc, _, _ := newIngestSvc(t, config.Config{MaxBodyBytes: 5})
	_, err := svc.ProcessPackage(context.Background(), 1, []byte("123456"), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "too large")
}

func TestProcessPackageXML(t *testing.T) {
	svc, repo, _ := newIngestSvc(t, config.Config{MaxBodyBytes: 1024 * 1024})
	body := []byte(`<?xml version="1.0"?><root/>`)
	pkg, err := svc.ProcessPackage(context.Background(), 42, body, strPtr("f.xml"))
	require.NoError(t, err)
	require.Equal(t, ingestion.StorageSingle, pkg.StorageMode)
	require.Equal(t, 1, pkg.FileCount)

	stats, err := repo.GetSupplierStats(context.Background(), 42)
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.TotalPackages)
}

func TestProcessPackageSmallZip(t *testing.T) {
	svc, _, _ := newIngestSvc(t, config.Config{
		MaxBodyBytes:      16 * 1024 * 1024,
		ZipThresholdSize:  102400,
		ZipThresholdCount: 100,
	})
	xml := []byte(`<?xml version="1.0"?><a/>`)
	zipBody := makeTestZip(t, xml)
	pkg, err := svc.ProcessPackage(context.Background(), 1, zipBody, strPtr("z.zip"))
	require.NoError(t, err)
	require.Equal(t, ingestion.StorageZipWithMembers, pkg.StorageMode)
	require.GreaterOrEqual(t, len(pkg.Files), 2)
}

func TestProcessPackageLargeZipNoAsync(t *testing.T) {
	svc, _, _ := newIngestSvc(t, config.Config{
		MaxBodyBytes:        16 * 1024 * 1024,
		ZipThresholdSize:    10,
		LargeZipAsyncUnpack: false,
	})
	zipBody := makeTestZip(t, []byte(`<?xml version="1.0"?><a/>`))
	pkg, err := svc.ProcessPackage(context.Background(), 1, zipBody, strPtr("big.zip"))
	require.NoError(t, err)
	require.Equal(t, ingestion.StorageRawLarge, pkg.StorageMode)
	require.Len(t, pkg.Files, 1)
}

func TestUnpackZipSkipsPathTraversal(t *testing.T) {
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, err := w.Create("../evil.xml")
	require.NoError(t, err)
	_, err = f.Write([]byte("<x/>"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	members, err := ingestion.UnpackZip(buf.Bytes())
	require.NoError(t, err)
	require.Empty(t, members)
}

func TestCountZipEntries(t *testing.T) {
	zipBody := makeTestZip(t, []byte(`<a/>`))
	count, err := ingestion.CountZipEntries(zipBody)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestIsZip(t *testing.T) {
	require.True(t, ingestion.IsZip([]byte{0x50, 0x4b, 0x03, 0x04, 0x00}))
	require.False(t, ingestion.IsZip([]byte(`<?xml`)))
}

func TestDetectPayloadTooSmall(t *testing.T) {
	_, err := ingestion.DetectPayload([]byte("ab"))
	require.Error(t, err)
}
