package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ShardTotalBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lbf_shard_total_bytes",
			Help: "On-disk segment file size for this shard instance",
		},
		[]string{"shard_id", "role"},
	)
	ShardReadOnly = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lbf_shard_read_only",
			Help: "1 if shard rejects writes",
		},
		[]string{"shard_id", "role"},
	)
)

// SetShardStats updates shard gauge metrics.
func SetShardStats(shardID int, role string, readOnly bool, totalBytes int64) {
	labels := prometheus.Labels{
		"shard_id": strconv.Itoa(shardID),
		"role":     role,
	}
	ShardTotalBytes.With(labels).Set(float64(totalBytes))
	if readOnly {
		ShardReadOnly.With(labels).Set(1)
	} else {
		ShardReadOnly.With(labels).Set(0)
	}
}
