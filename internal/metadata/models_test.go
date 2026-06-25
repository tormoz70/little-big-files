package metadata_test

import (
	"testing"
	"time"

	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/stretchr/testify/require"
)

func TestSupplierStatsDedupRatio(t *testing.T) {
	require.Zero(t, metadata.SupplierStats{}.DedupRatio())

	s := metadata.SupplierStats{TotalRefs: 100, DuplicateRefs: 50}
	require.InDelta(t, 0.5, s.DedupRatio(), 0.001)
}

func TestSupplierStatsFields(t *testing.T) {
	now := time.Now().UTC()
	s := metadata.SupplierStats{
		SupplierID:    7,
		TotalPackages: 3,
		TotalRefs:     10,
		DuplicateRefs: 4,
		LastActivity:  now,
	}
	require.Equal(t, 7, s.SupplierID)
	require.Equal(t, int64(3), s.TotalPackages)
}
