package main

import (
	"testing"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/coordinator"
	"github.com/stretchr/testify/require"
)

func TestStartupRegistrationRequestForcesStandby(t *testing.T) {
	req := startupRegistrationRequest(config.Config{
		ShardUUID:         "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		ClusterKey:        "cluster-key",
		ShardAdvertiseURL: "http://shard-1:8080",
		ShardStartupState: "active",
	})

	require.Equal(t, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", req.ShardUUID)
	require.Equal(t, "cluster-key", req.ClusterKey)
	require.Equal(t, "http://shard-1:8080", req.PrimaryURL)
	require.Equal(t, string(coordinator.ShardStandby), req.StartupState)
}

func TestStartupRegistrationRequestDefaultsToStandby(t *testing.T) {
	req := startupRegistrationRequest(config.Config{})
	require.Equal(t, string(coordinator.ShardStandby), req.StartupState)
}
