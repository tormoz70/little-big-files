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
	require.Equal(t, "./data/segments", cfg.DataDir)
	require.Equal(t, int64(64*1024*1024), cfg.MaxBodyBytes)
	require.Equal(t, "memory", cfg.DedupBackend)
	require.True(t, cfg.CompressionEnabled)
	require.True(t, cfg.DedupRebuildOnStart)
}

func TestLoadFromEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("DATA_DIR", "/tmp/seg")
	t.Setenv("DEDUP_BACKEND", "postgres")
	t.Setenv("WRITE_BUFFER_INTERVAL", "250ms")
	t.Setenv("BLOOM_FALSE_POSITIVE", "0.01")
	t.Setenv("COMPRESSION_ENABLED", "false")
	t.Setenv("LARGE_ZIP_ASYNC_UNPACK", "false")

	cfg := config.Load()
	require.Equal(t, "/tmp/seg", cfg.DataDir)
	require.Equal(t, "postgres", cfg.DedupBackend)
	require.Equal(t, 250*time.Millisecond, cfg.WriteBufferInterval)
	require.InDelta(t, 0.01, cfg.BloomFalsePositive, 0.0001)
	require.False(t, cfg.CompressionEnabled)
	require.False(t, cfg.LargeZipAsyncUnpack)
}

func TestLoadIgnoresInvalidEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv("MAX_BODY_BYTES", "not-a-number")
	t.Setenv("WRITE_BUFFER_INTERVAL", "not-a-duration")
	cfg := config.Load()
	require.Equal(t, int64(64*1024*1024), cfg.MaxBodyBytes)
	require.Equal(t, 100*time.Millisecond, cfg.WriteBufferInterval)
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"DATA_DIR", "PG_DSN", "SEGMENT_MAX_SIZE", "ZIP_THRESHOLD_SIZE", "ZIP_THRESHOLD_COUNT",
		"MAX_BODY_BYTES", "HTTP_ADDR", "MIGRATIONS_PATH", "LARGE_ZIP_ASYNC_UNPACK",
		"UNPACK_WORKERS", "UNPACK_QUEUE_SIZE", "WRITE_BUFFER_MAX_BYTES", "WRITE_BUFFER_INTERVAL",
		"COMPRESSION_ENABLED", "COMPRESSION_MIN_SIZE", "EXAMPLES_DIR", "DEDUP_BACKEND",
		"ROCKSDB_PATH", "BLOOM_EXPECTED_ITEMS", "BLOOM_FALSE_POSITIVE", "DEDUP_REBUILD_ON_START",
	} {
		os.Unsetenv(k)
	}
}
