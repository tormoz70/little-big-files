package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncOnceDownloadsAndSkipsWhenChecksumMatches(t *testing.T) {
	payload := []byte("segment-data")
	checksum := sha256.Sum256(payload)
	checksumHex := hex.EncodeToString(checksum[:])
	var downloads int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "secret", r.Header.Get("X-Cluster-Key"))
		switch r.URL.Path {
		case "/v1/internal/segments":
			_ = json.NewEncoder(w).Encode([]segmentFile{
				{
					Name:   "segment_0001.dat",
					Size:   int64(len(payload)),
					SHA256: checksumHex,
				},
			})
		case "/v1/internal/segments/segment_0001.dat":
			atomic.AddInt32(&downloads, 1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	client := &http.Client{}
	require.NoError(t, syncOnce(context.Background(), client, srv.URL, "secret", dir))
	require.NoError(t, syncOnce(context.Background(), client, srv.URL, "secret", dir))

	data, err := os.ReadFile(filepath.Join(dir, "segment_0001.dat"))
	require.NoError(t, err)
	require.Equal(t, payload, data)
	require.EqualValues(t, 1, atomic.LoadInt32(&downloads))
}

func TestSyncOnceReturnsErrorOnListStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forbidden", http.StatusUnauthorized)
	}))
	defer srv.Close()

	err := syncOnce(context.Background(), &http.Client{}, srv.URL, "k", t.TempDir())
	require.Error(t, err)
	require.Contains(t, err.Error(), "list segments")
}

func TestDownloadSegmentReturnsErrorOnBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "missing", http.StatusNotFound)
	}))
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "segment_0001.dat")
	err := downloadSegment(context.Background(), &http.Client{}, srv.URL, "k", "segment_0001.dat", dest)
	require.Error(t, err)
}

