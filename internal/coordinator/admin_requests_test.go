package coordinator_test

import (
	"testing"

	"github.com/little-big-files/little-big-files/internal/coordinator"
	"github.com/stretchr/testify/require"
)

func TestParseShardState(t *testing.T) {
	state, err := coordinator.ParseShardState("ACTIVE")
	require.NoError(t, err)
	require.Equal(t, coordinator.ShardActive, state)

	_, err = coordinator.ParseShardState("invalid")
	require.Error(t, err)
}
