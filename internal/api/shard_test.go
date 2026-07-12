package api_test

import (
	"bytes"
	"encoding/json"
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

func TestReplicaRejectsWrite(t *testing.T) {
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 1024*1024)
	require.NoError(t, err)
	defer segments.Close()

	cfg := config.Config{MaxBodyBytes: 1024 * 1024, ShardRole: "replica", ShardReadOnly: true}
	blobs := storage.NewBlobStore(segments, nil, nil, nil)
	ingest := ingestion.NewService(cfg, repo, blobs)
	srv := api.NewShardServer(cfg, ingest, repo, blobs, segments)

	req := httptest.NewRequest(http.MethodPost, "/v1/packages?supplier_id=1", bytes.NewReader([]byte(`<?xml version="1.0"?><x/>`)))
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestInternalStats(t *testing.T) {
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 1024*1024)
	require.NoError(t, err)
	defer segments.Close()

	cfg := config.Config{ShardID: 3, ShardRole: "primary", MaxBodyBytes: 1024 * 1024, ClusterKey: "secret"}
	blobs := storage.NewBlobStore(segments, nil, nil, nil)
	ingest := ingestion.NewService(cfg, repo, blobs)
	srv := api.NewShardServer(cfg, ingest, repo, blobs, segments)

	req := httptest.NewRequest(http.MethodGet, "/v1/internal/stats", nil)
	req.Header.Set("X-Cluster-Key", "secret")
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestInternalRequiresClusterKey(t *testing.T) {
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 1024*1024)
	require.NoError(t, err)
	defer segments.Close()

	cfg := config.Config{ShardID: 3, ShardRole: "primary", MaxBodyBytes: 1024 * 1024, ClusterKey: "secret"}
	blobs := storage.NewBlobStore(segments, nil, nil, nil)
	ingest := ingestion.NewService(cfg, repo, blobs)
	srv := api.NewShardServer(cfg, ingest, repo, blobs, segments)

	// missing key
	req := httptest.NewRequest(http.MethodGet, "/v1/internal/segments", nil)
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	// wrong key
	req2 := httptest.NewRequest(http.MethodGet, "/v1/internal/segments", nil)
	req2.Header.Set("X-Cluster-Key", "nope")
	rec2 := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusUnauthorized, rec2.Code)

	// seal without key must not change state
	reqSeal := httptest.NewRequest(http.MethodPost, "/v1/internal/seal", nil)
	recSeal := httptest.NewRecorder()
	srv.Router().ServeHTTP(recSeal, reqSeal)
	require.Equal(t, http.StatusUnauthorized, recSeal.Code)
	require.False(t, srv.ShardGuard().ReadOnly())
}

func TestInternalDisabledWithoutClusterKey(t *testing.T) {
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 1024*1024)
	require.NoError(t, err)
	defer segments.Close()

	cfg := config.Config{ShardID: 3, ShardRole: "primary", MaxBodyBytes: 1024 * 1024}
	blobs := storage.NewBlobStore(segments, nil, nil, nil)
	ingest := ingestion.NewService(cfg, repo, blobs)
	srv := api.NewShardServer(cfg, ingest, repo, blobs, segments)

	req := httptest.NewRequest(http.MethodGet, "/v1/internal/stats", nil)
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestSealSetsReadOnly(t *testing.T) {
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 1024*1024)
	require.NoError(t, err)
	defer segments.Close()

	cfg := config.Config{ShardID: 0, ShardRole: "primary", MaxBodyBytes: 1024 * 1024, ClusterKey: "secret"}
	blobs := storage.NewBlobStore(segments, nil, nil, nil)
	ingest := ingestion.NewService(cfg, repo, blobs)
	srv := api.NewShardServer(cfg, ingest, repo, blobs, segments)

	req := httptest.NewRequest(http.MethodPost, "/v1/internal/seal", nil)
	req.Header.Set("X-Cluster-Key", "secret")
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req2 := httptest.NewRequest(http.MethodPost, "/v1/packages?supplier_id=1", bytes.NewReader([]byte(`<?xml version="1.0"?><x/>`)))
	rec2 := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusForbidden, rec2.Code)
}

func TestDiskGateBlocksWritesButKeepsInternalStats(t *testing.T) {
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 1024*1024)
	require.NoError(t, err)
	defer segments.Close()

	cfg := config.Config{ShardID: 0, ShardRole: "primary", MaxBodyBytes: 1024 * 1024, ClusterKey: "secret"}
	blobs := storage.NewBlobStore(segments, nil, nil, nil)
	ingest := ingestion.NewService(cfg, repo, blobs)
	srv := api.NewShardServer(cfg, ingest, repo, blobs, segments)
	srv.ShardGuard().SetWriteBlockReason("disk_full")

	req := httptest.NewRequest(http.MethodPost, "/v1/packages?supplier_id=1", bytes.NewReader([]byte(`<?xml version="1.0"?><x/>`)))
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusInsufficientStorage, rec.Code)
	require.Contains(t, rec.Body.String(), "insufficient_storage")

	statsReq := httptest.NewRequest(http.MethodGet, "/v1/internal/stats", nil)
	statsReq.Header.Set("X-Cluster-Key", "secret")
	statsRec := httptest.NewRecorder()
	srv.Router().ServeHTTP(statsRec, statsReq)
	require.Equal(t, http.StatusOK, statsRec.Code)

	var stats map[string]any
	require.NoError(t, json.NewDecoder(statsRec.Body).Decode(&stats))
	require.Equal(t, "disk_full", stats["write_block_reason"])
}
