package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/little-big-files/little-big-files/internal/metrics"
	"github.com/stretchr/testify/require"
)

func TestMetricsHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "text/plain")
	body := rec.Body.String()
	require.True(t, strings.Contains(body, "lbf_http_requests_total") || strings.Contains(body, "go_goroutines"))
}

func TestMiddlewareRecordsRequest(t *testing.T) {
	handler := metrics.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1/packages", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)

	req2 := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec2 := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec2, req2)
	require.Contains(t, rec2.Body.String(), `lbf_http_requests_total`)
}

func TestShardHandlerExcludesCoordinatorMetrics(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	require.NotContains(t, rec.Body.String(), "lbf_coordinator_active_shard_id")
	require.NotContains(t, rec.Body.String(), "lbf_coordinator_shard_info")
}

func TestCoordinatorHandlerIncludesCoordinatorMetrics(t *testing.T) {
	metrics.SetCoordinatorShards([]metrics.ShardSnapshot{
		{ShardID: 0, State: "active", TotalBytes: 123},
		{ShardID: 1, State: "standby", TotalBytes: 0},
	}, 52_428_800)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.CoordinatorHandler().ServeHTTP(rec, req)
	body := rec.Body.String()
	require.Contains(t, body, "lbf_coordinator_active_shard_id 0")
	require.Contains(t, body, `lbf_coordinator_shard_info{shard_id="0",state="active"} 1`)
	require.Contains(t, body, `lbf_coordinator_shard_bar_bytes{shard_id="0",state="active"} 123`)
	require.Contains(t, body, `lbf_coordinator_shard_bar_bytes{shard_id="1",state="standby"} 0`)
}

func TestCoordinatorBarBytesShowsLastFourShards(t *testing.T) {
	metrics.SetCoordinatorShards([]metrics.ShardSnapshot{
		{ShardID: 0, State: "sealed", TotalBytes: 10},
		{ShardID: 1, State: "sealed", TotalBytes: 20},
		{ShardID: 2, State: "active", TotalBytes: 30},
		{ShardID: 3, State: "standby", TotalBytes: 40},
		{ShardID: 4, State: "standby", TotalBytes: 50},
		{ShardID: 5, State: "standby", TotalBytes: 60},
	}, 0)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.CoordinatorHandler().ServeHTTP(rec, req)
	body := rec.Body.String()
	require.NotContains(t, body, `lbf_coordinator_shard_bar_bytes{shard_id="0"`)
	require.NotContains(t, body, `lbf_coordinator_shard_bar_bytes{shard_id="1"`)
	require.Contains(t, body, `lbf_coordinator_shard_bar_bytes{shard_id="2",state="active"} 30`)
	require.Contains(t, body, `lbf_coordinator_shard_bar_bytes{shard_id="5",state="standby"} 60`)
}
