package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/little-big-files/little-big-files/internal/api"
	"github.com/little-big-files/little-big-files/internal/compress"
	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/coordinator"
	"github.com/little-big-files/little-big-files/internal/dedup"
	"github.com/little-big-files/little-big-files/internal/ingestion"
	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/little-big-files/little-big-files/internal/metrics"
	"github.com/little-big-files/little-big-files/internal/recovery"
	"github.com/little-big-files/little-big-files/internal/storage"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	if cfg.ShardRole != "replica" {
		if err := metadata.RunMigrations(ctx, cfg.PGDSN, cfg.MigrationsPath); err != nil {
			slog.Error("migrations failed", "err", err)
			os.Exit(1)
		}
	}

	repo, err := metadata.NewPostgresRepository(ctx, cfg.PGDSN)
	if err != nil {
		slog.Error("postgres connect failed", "err", err)
		os.Exit(1)
	}
	defer repo.Close()

	segments, err := storage.NewSegmentManager(cfg.DataDir, cfg.SegmentMaxSize)
	if err != nil {
		slog.Error("segment manager failed", "err", err)
		os.Exit(1)
	}
	defer segments.Close()

	segmentIndex := storage.NewSegmentIndex(cfg.DataDir)
	defer segmentIndex.Close()

	if cfg.WriteBufferMaxBytes > 0 {
		wb := storage.NewWriteBuffer(segments, cfg.WriteBufferMaxBytes, cfg.WriteBufferInterval)
		segments.SetWriteBuffer(wb)
		slog.Info("write buffer enabled", "max_bytes", cfg.WriteBufferMaxBytes, "interval", cfg.WriteBufferInterval)
	}

	encoder, err := compress.BootstrapEncoder(ctx, cfg, repo)
	if err != nil {
		slog.Error("compression init failed", "err", err)
		os.Exit(1)
	}
	if encoder != nil {
		defer encoder.Close()
	}

	dedupIdx, err := dedup.Open(cfg)
	if err != nil {
		slog.Error("dedup index open failed", "err", err)
		os.Exit(1)
	}
	if dedupIdx != nil {
		defer dedupIdx.Close()
		if cfg.DedupRebuildOnStart {
			if err := dedup.RebuildFromPG(ctx, dedupIdx, repo, cfg.BloomExpectedItems, cfg.BloomFalsePositive); err != nil {
				slog.Error("dedup index rebuild failed", "err", err)
				os.Exit(1)
			}
		}
		slog.Info("dedup index enabled", "backend", dedupIdx.Backend())
	}

	blobs := storage.NewBlobStore(segments, segmentIndex, encoder, dedupIdx)
	ingest := ingestion.NewService(cfg, repo, blobs)

	if cfg.ShardRole != "replica" {
		journal, err := recovery.NewJournal(cfg.DataDir)
		if err != nil {
			slog.Error("journal init failed", "err", err)
			os.Exit(1)
		}
		defer journal.Close()
		ingest.SetJournal(journal)
	}

	var unpackQ *ingestion.UnpackQueue
	if cfg.LargeZipAsyncUnpack && cfg.ShardRole != "replica" && !cfg.ShardReadOnly {
		unpackQ = ingestion.NewUnpackQueue(ingest, cfg.UnpackWorkers, cfg.UnpackQueueSize)
		ingest.SetUnpackQueue(unpackQ)
		defer unpackQ.Shutdown()
	}

	if cfg.ShardRole == "primary" && cfg.CoordinatorURL != "" {
		if cfg.ShardUUID == "" {
			slog.Error("SHARD_UUID is required when COORDINATOR_URL is set")
			os.Exit(1)
		}
		if cfg.ShardClusterKey == "" {
			slog.Error("SHARD_CLUSTER_KEY is required when COORDINATOR_URL is set")
			os.Exit(1)
		}
		if cfg.ShardAdvertiseURL == "" {
			slog.Error("SHARD_ADVERTISE_URL is required when COORDINATOR_URL is set")
			os.Exit(1)
		}
		startupState := cfg.ShardStartupState
		if startupState == "" {
			startupState = string(coordinator.ShardStandby)
		}
		resp, err := coordinator.RegisterShardWithRetry(ctx, cfg.CoordinatorURL, coordinator.RegisterShardRequest{
			ShardUUID:    cfg.ShardUUID,
			ClusterKey:   cfg.ShardClusterKey,
			PrimaryURL:   cfg.ShardAdvertiseURL,
			StartupState: startupState,
		})
		if err != nil {
			slog.Error("shard registration failed", "err", err)
			os.Exit(1)
		}
		cfg.ShardID = resp.Shard.ShardID
		if resp.Shard.State == coordinator.ShardSealed {
			cfg.ShardReadOnly = true
		}
		slog.Info("shard registered in coordinator",
			"shard_uuid", cfg.ShardUUID,
			"shard_id", cfg.ShardID,
			"state", resp.Shard.State,
			"registered", resp.Registered)
	}

	srv := api.NewShardServer(cfg, ingest, repo, blobs, segments)
	if guard := srv.ShardGuard(); guard != nil {
		var blobStats metrics.BlobByteTotalsProvider
		if cfg.ShardRole == "primary" {
			blobStats = repo
		}
		metrics.RunShardRefresh(ctx, guard, segments, blobStats, 10*time.Second)
	}

	httpSrv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      srv.Router(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	go func() {
		slog.Info("listening", "addr", cfg.HTTPAddr, "shard_id", cfg.ShardID, "role", cfg.ShardRole)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}
