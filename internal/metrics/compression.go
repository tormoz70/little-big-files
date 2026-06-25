package metrics

import (
	"strconv"

	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ShardBlobLogicalBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lbf_shard_blob_logical_bytes",
			Help: "Sum of uncompressed blob sizes (unique content_blobs.size)",
		},
		[]string{"shard_id", "role"},
	)
	ShardBlobStoredBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lbf_shard_blob_stored_bytes",
			Help: "Sum of on-disk blob record sizes (unique blobs, incl. record header)",
		},
		[]string{"shard_id", "role"},
	)
	ShardBlobReferencedLogicalBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lbf_shard_blob_referenced_logical_bytes",
			Help: "Sum of size * ref_count — logical bytes including dedup references",
		},
		[]string{"shard_id", "role"},
	)
	ShardCompressionRatio = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lbf_shard_compression_ratio",
			Help: "logical_bytes / stored_bytes; >1 means compression saved space on unique blobs",
		},
		[]string{"shard_id", "role"},
	)
	ShardCompressionSavingsPercent = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lbf_shard_compression_savings_percent",
			Help: "Percent disk space saved by compression on unique blobs: (1 - stored/logical) * 100",
		},
		[]string{"shard_id", "role"},
	)
	ShardStorageEfficiencyRatio = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lbf_shard_storage_efficiency_ratio",
			Help: "referenced_logical_bytes / segment_bytes; >1 includes dedup + compression benefit",
		},
		[]string{"shard_id", "role"},
	)
)

// SetCompressionStats updates compression and storage efficiency gauges.
func SetCompressionStats(shardID int, role string, totals metadata.BlobByteTotals, segmentBytes int64) {
	labels := prometheus.Labels{
		"shard_id": strconv.Itoa(shardID),
		"role":     role,
	}
	ShardBlobLogicalBytes.With(labels).Set(float64(totals.LogicalBytes))
	ShardBlobStoredBytes.With(labels).Set(float64(totals.StoredBytes))
	ShardBlobReferencedLogicalBytes.With(labels).Set(float64(totals.ReferencedLogicalBytes))

	if totals.StoredBytes > 0 {
		ShardCompressionRatio.With(labels).Set(float64(totals.LogicalBytes) / float64(totals.StoredBytes))
	} else {
		ShardCompressionRatio.With(labels).Set(1)
	}
	if totals.LogicalBytes > 0 {
		saved := (1 - float64(totals.StoredBytes)/float64(totals.LogicalBytes)) * 100
		if saved < 0 {
			saved = 0
		}
		ShardCompressionSavingsPercent.With(labels).Set(saved)
	} else {
		ShardCompressionSavingsPercent.With(labels).Set(0)
	}
	if segmentBytes > 0 {
		ShardStorageEfficiencyRatio.With(labels).Set(float64(totals.ReferencedLogicalBytes) / float64(segmentBytes))
	} else {
		ShardStorageEfficiencyRatio.With(labels).Set(1)
	}
}
