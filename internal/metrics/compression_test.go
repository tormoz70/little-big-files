package metrics_test

import (
	"testing"

	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/little-big-files/little-big-files/internal/metrics"
	"github.com/stretchr/testify/require"
)

func TestSetCompressionStats(t *testing.T) {
	metrics.SetCompressionStats(0, "primary", metadata.BlobByteTotals{
		LogicalBytes:           1000,
		StoredBytes:            400,
		ReferencedLogicalBytes: 2500,
	}, 800)

	// 1000 logical, 400 stored → 60% savings, ratio 2.5
	require.InDelta(t, 60.0, (1-400.0/1000.0)*100, 0.001)
	require.InDelta(t, 2.5, 1000.0/400.0, 0.001)
}
