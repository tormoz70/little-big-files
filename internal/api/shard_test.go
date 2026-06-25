package api_test

import (
	"bytes"
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

	cfg := config.Config{ShardID: 3, ShardRole: "primary", MaxBodyBytes: 1024 * 1024}
	blobs := storage.NewBlobStore(segments, nil, nil, nil)
	ingest := ingestion.NewService(cfg, repo, blobs)
	srv := api.NewShardServer(cfg, ingest, repo, blobs, segments)

	req := httptest.NewRequest(http.MethodGet, "/v1/internal/stats", nil)
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestSealSetsReadOnly(t *testing.T) {
	repo := testmetadata.NewMemoryRepository()
	segDir := t.TempDir()
	segments, err := storage.NewSegmentManager(segDir, 1024*1024)
	require.NoError(t, err)
	defer segments.Close()

	cfg := config.Config{ShardID: 0, ShardRole: "primary", MaxBodyBytes: 1024 * 1024}
	blobs := storage.NewBlobStore(segments, nil, nil, nil)
	ingest := ingestion.NewService(cfg, repo, blobs)
	srv := api.NewShardServer(cfg, ingest, repo, blobs, segments)

	req := httptest.NewRequest(http.MethodPost, "/v1/internal/seal", nil)
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req2 := httptest.NewRequest(http.MethodPost, "/v1/packages?supplier_id=1", bytes.NewReader([]byte(`<?xml version="1.0"?><x/>`)))
	rec2 := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusForbidden, rec2.Code)
}
