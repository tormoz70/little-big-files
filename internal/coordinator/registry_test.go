package coordinator_test

import (
	"testing"

	"github.com/little-big-files/little-big-files/internal/coordinator"
	"github.com/little-big-files/little-big-files/internal/globalid"
	"github.com/stretchr/testify/require"
)

func TestGlobalIDEncode(t *testing.T) {
	global := globalid.Encode(2, 42)
	shard, local := globalid.Decode(global)
	require.Equal(t, 2, shard)
	require.Equal(t, int64(42), local)
}

func TestRegistryReadURLPrefersReplicaForSealed(t *testing.T) {
	replica := "http://replica:8080"
	shard := coordinator.ShardInfo{
		ShardID:    0,
		State:      coordinator.ShardSealed,
		PrimaryURL: "http://primary:8080",
		ReplicaURL: &replica,
	}
	reg := coordinator.NewRegistry(nil, 0)
	require.Equal(t, replica, reg.ReadURL(&shard))
	require.Equal(t, "http://primary:8080", reg.ReadURL(&coordinator.ShardInfo{
		ShardID: 1, State: coordinator.ShardActive, PrimaryURL: "http://primary:8080",
	}))
}

func TestRegistryReadURLFallsBackToPrimaryWhenNoReplica(t *testing.T) {
	reg := coordinator.NewRegistry(nil, 0)
	primary := "http://primary:8080"
	shard := coordinator.ShardInfo{
		ShardID:    0,
		State:      coordinator.ShardSealed,
		PrimaryURL: primary,
	}
	require.Equal(t, primary, reg.ReadURL(&shard))
}
