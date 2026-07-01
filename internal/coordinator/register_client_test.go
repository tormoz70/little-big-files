package coordinator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/little-big-files/little-big-files/internal/coordinator"
	"github.com/stretchr/testify/require"
)

func TestRegisterShardOnceSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/v1/admin/shards", r.URL.Path)
		var req coordinator.RegisterShardRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", req.ShardUUID)

		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(coordinator.RegisterShardResponse{
			Shard: coordinator.ShardInfo{
				ShardID:    3,
				State:      coordinator.ShardStandby,
				PrimaryURL: req.PrimaryURL,
			},
			Registered: true,
		})
	}))
	defer srv.Close()

	resp, err := coordinator.RegisterShardOnce(context.Background(), srv.URL, coordinator.RegisterShardRequest{
		ShardUUID:    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		ClusterKey:   "k",
		PrimaryURL:   "http://shard-3:8080",
		StartupState: "standby",
	})
	require.NoError(t, err)
	require.True(t, resp.Registered)
	require.Equal(t, 3, resp.Shard.ShardID)
}

func TestRegisterShardOnceReturnsErrorOnBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := coordinator.RegisterShardOnce(context.Background(), srv.URL, coordinator.RegisterShardRequest{
		ShardUUID:  "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
		ClusterKey: "bad",
		PrimaryURL: "http://shard-x:8080",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "status=403")
}

func TestRegisterShardWithRetryFailsFastOnClientError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_, err := coordinator.RegisterShardWithRetry(ctx, srv.URL, coordinator.RegisterShardRequest{
		ShardUUID:  "cccccccc-cccc-cccc-cccc-cccccccccccc",
		ClusterKey: "bad",
		PrimaryURL: "http://shard-x:8080",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "status=403")
	require.Less(t, time.Since(start), 2*time.Second)
}

func TestRegisterShardWithRetryRetriesServerErrors(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			http.Error(w, "temporary", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(coordinator.RegisterShardResponse{
			Shard: coordinator.ShardInfo{
				ShardID:    7,
				State:      coordinator.ShardStandby,
				PrimaryURL: "http://shard-7:8080",
			},
			Registered: true,
		})
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := coordinator.RegisterShardWithRetry(ctx, srv.URL, coordinator.RegisterShardRequest{
		ShardUUID:  "dddddddd-dddd-dddd-dddd-dddddddddddd",
		ClusterKey: "ok",
		PrimaryURL: "http://shard-7:8080",
	})
	require.NoError(t, err)
	require.Equal(t, 7, resp.Shard.ShardID)
	require.Equal(t, 2, attempts)
}
