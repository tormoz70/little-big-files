package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DataDir            string
	PGDSN              string
	SegmentMaxSize     int64
	ZipThresholdSize   int
	ZipThresholdCount  int
	MaxBodyBytes       int64
	HTTPAddr           string
	MigrationsPath     string
	LargeZipAsyncUnpack bool
	UnpackWorkers       int
	UnpackQueueSize     int
	WriteBufferMaxBytes int
	WriteBufferInterval time.Duration
	CompressionEnabled  bool
	CompressionMinSize  int
	ExamplesDir         string
	DedupBackend        string
	RocksDBPath         string
	BloomExpectedItems  uint
	BloomFalsePositive  float64
	DedupRebuildOnStart bool
	// Shard (F4)
	ShardID             int
	ShardRole           string // primary | replica
	ShardReadOnly       bool
	ShardUUID           string
	ShardClusterKey     string
	ShardAdvertiseURL   string
	ShardStartupState   string
	CoordinatorURL      string
	// Coordinator (F4)
	CoordinatorPGDSN     string
	ShardMaxBytes        int64
	SealCheckInterval    time.Duration
	CoordinatorBootstrap string // shard registry bootstrap file or inline
	ClusterKey           string
}

func Load() Config {
	return Config{
		DataDir:           env("DATA_DIR", "./data/segments"),
		PGDSN:             env("PG_DSN", "postgres://lbf:lbf@localhost:5432/lbf?sslmode=disable"),
		SegmentMaxSize:    envInt64("SEGMENT_MAX_SIZE", 4*1024*1024*1024),
		ZipThresholdSize:  envInt("ZIP_THRESHOLD_SIZE", 102400),
		ZipThresholdCount: envInt("ZIP_THRESHOLD_COUNT", 100),
		MaxBodyBytes:      envInt64("MAX_BODY_BYTES", 64*1024*1024),
		HTTPAddr:            env("HTTP_ADDR", ":8080"),
		MigrationsPath:      env("MIGRATIONS_PATH", "./migrations"),
		LargeZipAsyncUnpack: envBool("LARGE_ZIP_ASYNC_UNPACK", true),
		UnpackWorkers:       envInt("UNPACK_WORKERS", 2),
		UnpackQueueSize:     envInt("UNPACK_QUEUE_SIZE", 256),
		WriteBufferMaxBytes: envInt("WRITE_BUFFER_MAX_BYTES", 4*1024*1024),
		WriteBufferInterval: envDuration("WRITE_BUFFER_INTERVAL", 100*time.Millisecond),
		CompressionEnabled:  envBool("COMPRESSION_ENABLED", true),
		CompressionMinSize:  envInt("COMPRESSION_MIN_SIZE", 64),
		ExamplesDir:         env("EXAMPLES_DIR", "./examples"),
		DedupBackend:        env("DEDUP_BACKEND", "memory"),
		RocksDBPath:         env("ROCKSDB_PATH", "./data/rocksdb"),
		BloomExpectedItems:  uint(envInt("BLOOM_EXPECTED_ITEMS", 1_000_000)),
		BloomFalsePositive:  envFloat("BLOOM_FALSE_POSITIVE", 0.001),
		DedupRebuildOnStart: envBool("DEDUP_REBUILD_ON_START", true),
		ShardID:             envInt("SHARD_ID", 0),
		ShardRole:           env("SHARD_ROLE", "primary"),
		ShardReadOnly:       envBool("SHARD_READ_ONLY", false),
		ShardUUID:           env("SHARD_UUID", ""),
		ShardClusterKey:     env("SHARD_CLUSTER_KEY", ""),
		ShardAdvertiseURL:   env("SHARD_ADVERTISE_URL", ""),
		ShardStartupState:   env("SHARD_STARTUP_STATE", "standby"),
		CoordinatorURL:      env("COORDINATOR_URL", ""),
		CoordinatorPGDSN:    env("COORDINATOR_PG_DSN", "postgres://lbf:lbf@localhost:5433/coordinator?sslmode=disable"),
		ShardMaxBytes:       envInt64("SHARD_MAX_BYTES", 500*1024*1024*1024),
		SealCheckInterval:   envDuration("SEAL_CHECK_INTERVAL", 30*time.Second),
		CoordinatorBootstrap: env("COORDINATOR_BOOTSTRAP", "./deploy/shards.bootstrap.json"),
		ClusterKey:           env("CLUSTER_KEY", ""),
	}
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return def
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return n
		}
	}
	return def
}
