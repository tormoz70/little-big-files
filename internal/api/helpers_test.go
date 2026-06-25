package api

import (
	"errors"
	"testing"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/ingestion"
	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/stretchr/testify/require"
)

func TestContentTypeFor(t *testing.T) {
	require.Equal(t, "application/zip", contentTypeFor(ingestion.RoleOriginal, "pkg.zip"))
	require.Equal(t, "application/xml", contentTypeFor(ingestion.RoleMember, "a.xml"))
	require.Equal(t, "text/plain; charset=utf-8", contentTypeFor(ingestion.RoleUnpackError, "_unpack_error.txt"))
	require.Equal(t, "application/octet-stream", contentTypeFor("other", "data.bin"))
	require.Equal(t, "application/xml", contentTypeFor("other", "x.XML"))
}

func TestIsClientError(t *testing.T) {
	require.True(t, isClientError(errors.New("empty body")))
	require.True(t, isClientError(errors.New("unsupported payload")))
	require.True(t, isClientError(errors.New("payload too large")))
	require.False(t, isClientError(errors.New("postgres down")))
}

func TestBuildPackageResponseLargeZipPending(t *testing.T) {
	cfg := config.Config{LargeZipAsyncUnpack: true}
	pkg := &metadata.Package{
		ID:          1,
		SupplierID:  2,
		PayloadType: string(ingestion.PayloadZIP),
		StorageMode: ingestion.StorageRawLarge,
		FileCount:   1,
	}
	resp := buildPackageResponse(cfg, pkg)
	require.Equal(t, ingestion.UnpackPending, resp.UnpackStatus)
}

func TestBuildPackageResponseLargeZipSkipped(t *testing.T) {
	cfg := config.Config{LargeZipAsyncUnpack: false}
	pkg := &metadata.Package{
		ID:          1,
		PayloadType: string(ingestion.PayloadZIP),
		StorageMode: ingestion.StorageRawLarge,
	}
	resp := buildPackageResponse(cfg, pkg)
	require.Equal(t, ingestion.UnpackSkipped, resp.UnpackStatus)
}

func TestBuildPackageResponseUnpackFailed(t *testing.T) {
	msg := "bad zip"
	pkg := &metadata.Package{
		ID:          1,
		PayloadType: string(ingestion.PayloadZIP),
		StorageMode: ingestion.StorageZipWithMembers,
		UnpackError: &msg,
		Files: []metadata.PackageFile{{
			ID: 10, Role: ingestion.RoleOriginal, Size: 5,
		}},
	}
	resp := buildPackageResponse(config.Config{}, pkg)
	require.Equal(t, ingestion.UnpackFailed, resp.UnpackStatus)
	require.Equal(t, msg, resp.UnpackError)
	require.Len(t, resp.Files, 1)
	require.Equal(t, "/v1/packages/1/files/10", resp.Files[0].DownloadURL)
}
