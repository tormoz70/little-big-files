package coordinator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
