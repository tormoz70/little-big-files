package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/coordinator"
	"github.com/little-big-files/little-big-files/internal/metrics"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	migrations := "./migrations/coordinator"
	if v := os.Getenv("COORDINATOR_MIGRATIONS_PATH"); v != "" {
		migrations = v
	}
	if err := coordinator.RunMigrations(ctx, cfg.CoordinatorPGDSN, migrations); err != nil {
		slog.Error("coordinator migrations failed", "err", err)
		os.Exit(1)
	}

	repo, err := coordinator.NewRepository(ctx, cfg.CoordinatorPGDSN)
	if err != nil {
		slog.Error("coordinator postgres failed", "err", err)
		os.Exit(1)
	}
	defer repo.Close()

	if err := repo.BootstrapFromFile(ctx, cfg.CoordinatorBootstrap); err != nil {
		slog.Error("shard bootstrap failed", "err", err)
		os.Exit(1)
	}

	reg := coordinator.NewRegistry(repo, cfg.ShardMaxBytes)
	srv := coordinator.NewServer(cfg, reg)
	srv.RunSealLoop(ctx)
	metrics.RunCoordinatorRefresh(ctx, reg, cfg.ShardMaxBytes, 10*time.Second)

	httpSrv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      srv.Router(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	go func() {
		slog.Info("coordinator listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("coordinator error", "err", err)
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
