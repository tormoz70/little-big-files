package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/coordinator"
	"github.com/little-big-files/little-big-files/internal/metadata"
)

func main() {
	runMetadata := flag.Bool("metadata", true, "run shard/standalone metadata migrations")
	runCoordinator := flag.Bool("coordinator", false, "run coordinator migrations")
	coordinatorMigrations := flag.String("coordinator-migrations-path", "./migrations/coordinator", "path to coordinator migration files")
	flag.Parse()

	cfg := config.Load()
	ctx := context.Background()

	if *runMetadata {
		if err := metadata.RunMigrations(ctx, cfg.PGDSN, cfg.MigrationsPath); err != nil {
			slog.Error("metadata migrations failed", "err", err)
			os.Exit(1)
		}
	}

	if *runCoordinator {
		if err := coordinator.RunMigrations(ctx, cfg.CoordinatorPGDSN, *coordinatorMigrations); err != nil {
			slog.Error("coordinator migrations failed", "err", err)
			os.Exit(1)
		}
	}
}

