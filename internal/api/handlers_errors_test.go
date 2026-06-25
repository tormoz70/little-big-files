package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/little-big-files/little-big-files/internal/api"
	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/ingestion"
	"github.com/little-big-files/little-big-files/internal/storage"
	"github.com/little-big-files/little-big-files/internal/testmetadata"
	"github.com/stretchr/testify/require"
)

func TestPostPackageMissingSupplierID(t *testing.T) {
	env := setupHandlerEnv(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/packages", bytes.NewReader([]byte("x")))
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestPostPackageInvalidSupplierID(t *testing.T) {
	env := setupHandlerEnv(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/packages?supplier_id=0", bytes.NewReader([]byte("x")))
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	req2 := httptest.NewRequest(http.MethodPost, "/v1/packages?supplier_id=1000001", bytes.NewReader([]byte("x")))
	rec2 := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestPostPackageEmptyBody(t *testing.T) {
	env := setupHandlerEnv(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/packages?supplier_id=1", bytes.NewReader(nil))
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestPostPackageTooLarge(t *testing.T) {
	env := setupHandlerEnvWithMaxBody(t, 10)
	req := httptest.NewRequest(http.MethodPost, "/v1/packages?supplier_id=1", bytes.NewReader([]byte(strings.Repeat("a", 20))))
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
}

func setupHandlerEnvWithMaxBody(t *testing.T, max int64) *handlerEnv {
	t.Helper()
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 64*1024*1024)
	require.NoError(t, err)
	t.Cleanup(func() { _ = segments.Close() })
	cfg := config.Config{MaxBodyBytes: max, ZipThresholdSize: 102400, ZipThresholdCount: 100}
	blobs := storage.NewBlobStore(segments, nil, nil, nil)
	ingest := ingestion.NewService(cfg, repo, blobs)
	srv := api.NewServer(cfg, ingest, repo, blobs)
	return &handlerEnv{server: srv, repo: repo}
}

func TestGetPackageNotFound(t *testing.T) {
	env := setupHandlerEnv(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/packages/9999", nil)
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestGetPackageInvalidID(t *testing.T) {
	env := setupHandlerEnv(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/packages/not-a-number", nil)
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetFileNotFound(t *testing.T) {
	env := setupHandlerEnv(t)
	body := []byte(`<?xml version="1.0"?><x/>`)
	resp := postPackage(t, env, 1, body, "a.xml")
	id := int64(resp["package_id"].(float64))

	req := httptest.NewRequest(http.MethodGet, "/v1/packages/"+itoa64(id)+"/files/99999", nil)
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestGetOriginalNotFound(t *testing.T) {
	env := setupHandlerEnv(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/packages/404/original", nil)
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestPostSmallZipUnpackOK(t *testing.T) {
	env := setupHandlerEnv(t)
	xml := []byte(`<?xml version="1.0"?><doc/>`)
	zipBody := makeZip(t, map[string][]byte{"m.xml": xml})
	resp := postPackage(t, env, 5, zipBody, "small.zip")
	require.Equal(t, "ok", resp["unpack_status"])
	require.Equal(t, "zip_with_members", resp["storage_mode"])
}

func TestPostLargeZipPending(t *testing.T) {
	env := setupLargeZipEnv(t, true)
	xml := []byte(`<?xml version="1.0"?><doc/>`)
	zipBody := makeZip(t, map[string][]byte{"m.xml": xml})
	resp := postPackage(t, env, 5, zipBody, "large.zip")
	require.Equal(t, "pending", resp["unpack_status"])
	require.Equal(t, "raw_large", resp["storage_mode"])
}

func TestPostLargeZipSkipped(t *testing.T) {
	env := setupLargeZipEnv(t, false)
	xml := []byte(`<?xml version="1.0"?><doc/>`)
	zipBody := makeZip(t, map[string][]byte{"m.xml": xml})
	resp := postPackage(t, env, 5, zipBody, "large.zip")
	require.Equal(t, "skipped", resp["unpack_status"])
}

func TestGetFileContentTypeXML(t *testing.T) {
	env := setupHandlerEnv(t)
	body := []byte(`<?xml version="1.0"?><x/>`)
	resp := postPackage(t, env, 1, body, "report.xml")
	id := int64(resp["package_id"].(float64))
	var fileID int64
	for _, f := range resp["files"].([]any) {
		m := f.(map[string]any)
		fileID = int64(m["file_id"].(float64))
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/packages/"+itoa64(id)+"/files/"+itoa64(fileID), nil)
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/xml", rec.Header().Get("Content-Type"))
}

func TestPostUnsupportedPayload(t *testing.T) {
	env := setupHandlerEnv(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/packages?supplier_id=1", bytes.NewReader([]byte("plain text not xml")))
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	require.NotEmpty(t, errResp["error"])
}

func setupLargeZipEnv(t *testing.T, asyncUnpack bool) *handlerEnv {
	t.Helper()
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 64*1024*1024)
	require.NoError(t, err)
	t.Cleanup(func() { _ = segments.Close() })
	cfg := config.Config{
		MaxBodyBytes:        16 * 1024 * 1024,
		ZipThresholdSize:    10,
		ZipThresholdCount:     100,
		LargeZipAsyncUnpack: asyncUnpack,
	}
	blobs := storage.NewBlobStore(segments, nil, nil, nil)
	ingest := ingestion.NewService(cfg, repo, blobs)
	srv := api.NewServer(cfg, ingest, repo, blobs)
	return &handlerEnv{server: srv, repo: repo}
}

func TestGetFileInvalidFileID(t *testing.T) {
	env := setupHandlerEnv(t)
	body := []byte(`<?xml version="1.0"?><x/>`)
	resp := postPackage(t, env, 1, body, "a.xml")
	id := int64(resp["package_id"].(float64))

	req := httptest.NewRequest(http.MethodGet, "/v1/packages/"+itoa64(id)+"/files/not-id", nil)
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}
