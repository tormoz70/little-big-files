package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/little-big-files/little-big-files/internal/metadata"
)

// SegmentStatsProvider supplies on-disk byte totals.
type SegmentStatsProvider interface {
	TotalBytes() (int64, error)
}

// BlobByteTotalsProvider reads logical vs stored blob sizes from metadata.
type BlobByteTotalsProvider interface {
	BlobByteTotals(ctx context.Context) (metadata.BlobByteTotals, error)
}

// ShardStatsProvider supplies shard identity and read-only flag.
type ShardStatsProvider interface {
	MetricsShardID() int
	MetricsRole() string
	ReadOnly() bool
}

// RunShardRefresh periodically updates shard gauge metrics.
func RunShardRefresh(
	ctx context.Context,
	shard ShardStatsProvider,
	segments SegmentStatsProvider,
	blobs BlobByteTotalsProvider,
	interval time.Duration,
) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	refresh := func() {
		total, err := segments.TotalBytes()
		if err != nil {
			slog.Warn("shard metrics refresh failed", "err", err)
			return
		}
		SetShardStats(shard.MetricsShardID(), shard.MetricsRole(), shard.ReadOnly(), total)

		if blobs != nil && shard.MetricsRole() == "primary" {
			totals, err := blobs.BlobByteTotals(ctx)
			if err != nil {
				slog.Warn("blob size metrics refresh failed", "err", err)
				return
			}
			SetCompressionStats(shard.MetricsShardID(), shard.MetricsRole(), totals, total)
		}
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
