package api_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/little-big-files/little-big-files/internal/api"
	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/ingestion"
	"github.com/little-big-files/little-big-files/internal/storage"
	"github.com/little-big-files/little-big-files/internal/testmetadata"
	"github.com/stretchr/testify/require"
)

type handlerEnv struct {
	server *api.Server
	repo   *testmetadata.MemoryRepository
}

func setupHandlerEnv(t *testing.T) *handlerEnv {
	t.Helper()
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 64*1024*1024)
	require.NoError(t, err)
	t.Cleanup(func() { _ = segments.Close() })

	cfg := config.Config{MaxBodyBytes: 16 * 1024 * 1024, ZipThresholdSize: 102400, ZipThresholdCount: 100}
	blobs := storage.NewBlobStore(segments, nil, nil)
	ingest := ingestion.NewService(cfg, repo, blobs)
	srv := api.NewServer(cfg, ingest, repo, blobs)
	return &handlerEnv{server: srv, repo: repo}
}

func makeZip(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for name, data := range files {
		f, err := w.Create(name)
		require.NoError(t, err)
		_, err = f.Write(data)
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func postPackage(t *testing.T, env *handlerEnv, supplierID int, body []byte, filename string) map[string]any {
	t.Helper()
	url := "/v1/packages?supplier_id=" + itoa(supplierID)
	if filename != "" {
		url += "&filename=" + filename
	}
	req := httptest.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	return resp
}

func getBytes(t *testing.T, env *handlerEnv, path string) []byte {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	data, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	return data
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func itoa64(n int64) string { return itoa(int(n)) }

func TestDuplicateXML100PackagesOneBlob(t *testing.T) {
	env := setupHandlerEnv(t)
	body := []byte(`<?xml version="1.0"?><seans ver="3.2.0"></seans>`)

	var firstID float64
	for i := 0; i < 100; i++ {
		resp := postPackage(t, env, 2447, body, "test.xml")
		id := resp["package_id"].(float64)
		if i == 0 {
			firstID = id
		} else {
			require.NotEqual(t, firstID, id)
		}
	}

	count, err := env.repo.CountContentBlobs(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestDuplicateZIPClone(t *testing.T) {
	env := setupHandlerEnv(t)
	xml := []byte(`<?xml version="1.0"?><seans></seans>`)
	zipBody := makeZip(t, map[string][]byte{"a.xml": xml})

	resp1 := postPackage(t, env, 2447, zipBody, "pkg.zip")
	id1 := int64(resp1["package_id"].(float64))

	resp2 := postPackage(t, env, 2447, zipBody, "pkg.zip")
	id2 := int64(resp2["package_id"].(float64))
	require.NotEqual(t, id1, id2)

	require.Equal(t, zipBody, getBytes(t, env, "/v1/packages/"+itoa64(id1)+"/original"))
	require.Equal(t, zipBody, getBytes(t, env, "/v1/packages/"+itoa64(id2)+"/original"))
}

func TestCorruptZIPUnpackError(t *testing.T) {
	env := setupHandlerEnv(t)
	body := []byte{0x50, 0x4b, 0x03, 0x04, 0x00, 0x00, 0x00}

	resp := postPackage(t, env, 2447, body, "broken.zip")
	require.Equal(t, "failed", resp["unpack_status"])
	require.NotEmpty(t, resp["unpack_error"])

	var errFileID int64
	for _, f := range resp["files"].([]any) {
		m := f.(map[string]any)
		if m["role"] == "unpack_error" {
			errFileID = int64(m["file_id"].(float64))
		}
	}
	require.NotZero(t, errFileID)

	pkgID := int64(resp["package_id"].(float64))
	errText := string(getBytes(t, env, "/v1/packages/"+itoa64(pkgID)+"/files/"+itoa64(errFileID)))
	require.Equal(t, resp["unpack_error"], errText)
}

func TestMemberLevelDedup(t *testing.T) {
	env := setupHandlerEnv(t)
	shared := []byte(`<?xml version="1.0"?><seans shared="1"></seans>`)
	zip1 := makeZip(t, map[string][]byte{"a.xml": shared, "b.xml": []byte(`<other/>`)})
	zip2 := makeZip(t, map[string][]byte{"c.xml": shared})

	postPackage(t, env, 1, zip1, "z1.zip")
	resp2 := postPackage(t, env, 2, zip2, "z2.zip")

	blobCount, err := env.repo.CountContentBlobs(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(4), blobCount)

	var memberID int64
	for _, f := range resp2["files"].([]any) {
		m := f.(map[string]any)
		if m["role"] == "member" && m["filename"] == "c.xml" {
			memberID = int64(m["file_id"].(float64))
		}
	}
	pkg2ID := int64(resp2["package_id"].(float64))
	require.Equal(t, shared, getBytes(t, env, "/v1/packages/"+itoa64(pkg2ID)+"/files/"+itoa64(memberID)))
}

func TestGETPackageMetadata(t *testing.T) {
	env := setupHandlerEnv(t)
	body := []byte(`<?xml version="1.0"?><x/>`)
	resp := postPackage(t, env, 107, body, "report.xml")
	id := int64(resp["package_id"].(float64))

	req := httptest.NewRequest(http.MethodGet, "/v1/packages/"+itoa64(id), nil)
	rec := httptest.NewRecorder()
	env.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var got map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, float64(107), got["supplier_id"])
	require.Equal(t, "single", got["storage_mode"])
}
