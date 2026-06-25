package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Coordinator metrics live on a dedicated registry — only exposed by cmd/coordinator.
var coordinatorRegistry = prometheus.NewRegistry()

var (
	CoordinatorShardInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lbf_coordinator_shard_info",
			Help: "Shard registry entry (value is always 1)",
		},
		[]string{"shard_id", "state"},
	)
	CoordinatorShardBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lbf_coordinator_shard_bytes",
			Help: "Reported total_bytes from shard registry",
		},
		[]string{"shard_id", "state"},
	)
	CoordinatorActiveShard = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "lbf_coordinator_active_shard_id",
			Help: "Currently active shard id",
		},
	)
	CoordinatorShardMaxBytes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "lbf_coordinator_shard_max_bytes",
			Help: "Seal threshold configured on coordinator",
		},
	)
	CoordinatorShardBarBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lbf_coordinator_shard_bar_bytes",
			Help: "total_bytes for the last up to 4 shards by shard_id (dashboard bar chart)",
		},
		[]string{"shard_id", "state"},
	)
)

func init() {
	coordinatorRegistry.MustRegister(
		CoordinatorShardInfo,
		CoordinatorShardBytes,
		CoordinatorActiveShard,
		CoordinatorShardMaxBytes,
		CoordinatorShardBarBytes,
	)
}

// CoordinatorHandler exposes HTTP metrics (default registry) and coordinator-only metrics.
func CoordinatorHandler() http.Handler {
	return promhttp.HandlerFor(
		prometheus.Gatherers{
			prometheus.DefaultGatherer,
			coordinatorRegistry,
		},
		promhttp.HandlerOpts{},
	)
}

// ShardSnapshot is a minimal view of registry state for metrics.
type ShardSnapshot struct {
	ShardID    int
	State      string
	TotalBytes int64
}

// RegistryReader supplies shard registry data for metrics refresh.
type RegistryReader interface {
	ShardSnapshots(ctx context.Context) ([]ShardSnapshot, error)
}

// SetCoordinatorShards updates coordinator gauges from registry snapshots.
func SetCoordinatorShards(shards []ShardSnapshot, maxBytes int64) {
	CoordinatorShardInfo.Reset()
	CoordinatorShardBytes.Reset()
	CoordinatorShardBarBytes.Reset()

	activeID := -1.0
	for _, s := range shards {
		id := strconv.Itoa(s.ShardID)
		CoordinatorShardInfo.WithLabelValues(id, s.State).Set(1)
		CoordinatorShardBytes.WithLabelValues(id, s.State).Set(float64(s.TotalBytes))
		if s.State == "active" {
			activeID = float64(s.ShardID)
		}
	}
	for _, s := range recentShardWindow(shards, 4) {
		id := strconv.Itoa(s.ShardID)
		CoordinatorShardBarBytes.WithLabelValues(id, s.State).Set(float64(s.TotalBytes))
	}
	if activeID >= 0 {
		CoordinatorActiveShard.Set(activeID)
	}
	CoordinatorShardMaxBytes.Set(float64(maxBytes))
}

func recentShardWindow(shards []ShardSnapshot, limit int) []ShardSnapshot {
	if len(shards) == 0 || limit <= 0 {
		return nil
	}
	sorted := append([]ShardSnapshot(nil), shards...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ShardID < sorted[j].ShardID })
	if len(sorted) <= limit {
		return sorted
	}
	return sorted[len(sorted)-limit:]
}

// RunCoordinatorRefresh periodically updates coordinator metrics.
func RunCoordinatorRefresh(ctx context.Context, reader RegistryReader, maxBytes int64, interval time.Duration) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	refresh := func() {
		shards, err := reader.ShardSnapshots(ctx)
		if err != nil {
			slog.Warn("coordinator metrics refresh failed", "err", err)
			return
		}
		SetCoordinatorShards(shards, maxBytes)
	}
	refresh()
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				refresh()
			}
		}
	}()
}
