package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/stretchr/testify/require"
)

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	cfg := config.Load()
	require.Equal(t, "single-node", cfg.DeploymentMode)
	require.Equal(t, "./data/segments", cfg.DataDir)
	require.Equal(t, int64(64*1024*1024), cfg.MaxBodyBytes)
	require.Equal(t, "memory", cfg.DedupBackend)
	require.True(t, cfg.CompressionEnabled)
	require.True(t, cfg.DedupRebuildOnStart)
	require.Equal(t, int64(0), cfg.MinFreeDiskBytes)
	require.Equal(t, 10*time.Second, cfg.DiskCheckInterval)
	require.Equal(t, int64(64*1024*1024), cfg.DiskResumeHysteresisBytes)
}

func TestLoadFromEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("DATA_DIR", "/tmp/seg")
	t.Setenv("DEDUP_BACKEND", "postgres")
	t.Setenv("WRITE_BUFFER_INTERVAL", "250ms")
	t.Setenv("BLOOM_FALSE_POSITIVE", "0.01")
	t.Setenv("COMPRESSION_ENABLED", "false")
	t.Setenv("LARGE_ZIP_ASYNC_UNPACK", "false")
	t.Setenv("SHARD_UUID", "11111111-1111-1111-1111-111111111111")
	t.Setenv("SHARD_CLUSTER_KEY", "secret")
	t.Setenv("COORDINATOR_URL", "http://coordinator:8080")
	t.Setenv("MIN_FREE_DISK_BYTES", "123")
	t.Setenv("DISK_CHECK_INTERVAL", "5s")
	t.Setenv("DISK_RESUME_HYSTERESIS_BYTES", "50")

	cfg := config.Load()
	require.Equal(t, "/tmp/seg", cfg.DataDir)
	require.Equal(t, "postgres", cfg.DedupBackend)
	require.Equal(t, 250*time.Millisecond, cfg.WriteBufferInterval)
	require.InDelta(t, 0.01, cfg.BloomFalsePositive, 0.0001)
	require.False(t, cfg.CompressionEnabled)
	require.False(t, cfg.LargeZipAsyncUnpack)
	require.Equal(t, "11111111-1111-1111-1111-111111111111", cfg.ShardUUID)
	require.Equal(t, "secret", cfg.ShardClusterKey)
	require.Equal(t, "http://coordinator:8080", cfg.CoordinatorURL)
	require.Equal(t, "sharded", cfg.DeploymentMode)
	require.Equal(t, int64(123), cfg.MinFreeDiskBytes)
	require.Equal(t, 5*time.Second, cfg.DiskCheckInterval)
	require.Equal(t, int64(50), cfg.DiskResumeHysteresisBytes)
}

func TestLoadIgnoresInvalidEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("MAX_BODY_BYTES", "not-a-number")
	t.Setenv("WRITE_BUFFER_INTERVAL", "not-a-duration")
	t.Setenv("MIN_FREE_DISK_BYTES", "nope")
	t.Setenv("DISK_CHECK_INTERVAL", "bad")
	t.Setenv("DISK_RESUME_HYSTERESIS_BYTES", "bad")
	cfg := config.Load()
	require.Equal(t, int64(64*1024*1024), cfg.MaxBodyBytes)
	require.Equal(t, 100*time.Millisecond, cfg.WriteBufferInterval)
	require.Equal(t, int64(0), cfg.MinFreeDiskBytes)
	require.Equal(t, 10*time.Second, cfg.DiskCheckInterval)
	require.Equal(t, int64(64*1024*1024), cfg.DiskResumeHysteresisBytes)
}

func TestDeploymentModeCanBeOverridden(t *testing.T) {
	clearEnv(t)
	t.Setenv("DEPLOYMENT_MODE", "Sharded")
	cfg := config.Load()
	require.Equal(t, "sharded", cfg.DeploymentMode)
	require.False(t, cfg.IsSingleNode())
}

func TestIsSingleNode(t *testing.T) {
	cfg := config.Config{DeploymentMode: "single-node"}
	require.True(t, cfg.IsSingleNode())
	require.False(t, (config.Config{DeploymentMode: "sharded"}).IsSingleNode())
}

func TestEffectiveClusterKeyPrefersClusterKey(t *testing.T) {
	cfg := config.Config{
		ClusterKey:     "cluster",
		ShardClusterKey: "shard",
	}
	require.Equal(t, "cluster", cfg.EffectiveClusterKey())
}

func TestEffectiveClusterKeyFallsBackToShardClusterKey(t *testing.T) {
	cfg := config.Config{
		ShardClusterKey: "shard-only",
	}
	require.Equal(t, "shard-only", cfg.EffectiveClusterKey())
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"DATA_DIR", "PG_DSN", "SEGMENT_MAX_SIZE", "ZIP_THRESHOLD_SIZE", "ZIP_THRESHOLD_COUNT",
		"MAX_BODY_BYTES", "HTTP_ADDR", "MIGRATIONS_PATH", "LARGE_ZIP_ASYNC_UNPACK",
		"UNPACK_WORKERS", "UNPACK_QUEUE_SIZE", "WRITE_BUFFER_MAX_BYTES", "WRITE_BUFFER_INTERVAL",
		"COMPRESSION_ENABLED", "COMPRESSION_MIN_SIZE", "EXAMPLES_DIR", "DEDUP_BACKEND",
		"ROCKSDB_PATH", "BLOOM_EXPECTED_ITEMS", "BLOOM_FALSE_POSITIVE", "DEDUP_REBUILD_ON_START",
		"SHARD_ID", "SHARD_ROLE", "SHARD_READ_ONLY", "SHARD_UUID", "SHARD_CLUSTER_KEY",
		"SHARD_ADVERTISE_URL", "SHARD_STARTUP_STATE", "COORDINATOR_URL", "CLUSTER_KEY",
		"COORDINATOR_PG_DSN", "SHARD_MAX_BYTES", "SEAL_CHECK_INTERVAL", "COORDINATOR_BOOTSTRAP",
		"DEPLOYMENT_MODE", "MIN_FREE_DISK_BYTES", "DISK_CHECK_INTERVAL", "DISK_RESUME_HYSTERESIS_BYTES",
	} {
		os.Unsetenv(k)
	}
}
