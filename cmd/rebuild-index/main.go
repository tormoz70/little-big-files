package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/dedup"
	"github.com/little-big-files/little-big-files/internal/metadata"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	repo, err := metadata.NewPostgresRepository(ctx, cfg.PGDSN)
	if err != nil {
		slog.Error("postgres connect failed", "err", err)
		os.Exit(1)
	}
	defer repo.Close()

	idx, err := dedup.Open(cfg)
	if err != nil {
		slog.Error("dedup index open failed", "err", err)
		os.Exit(1)
	}
	defer idx.Close()

	if err := dedup.RebuildFromPG(ctx, idx, repo, cfg.BloomExpectedItems, cfg.BloomFalsePositive); err != nil {
		slog.Error("rebuild failed", "err", err)
		os.Exit(1)
	}

	n, _ := idx.Len()
	slog.Info("rebuild complete", "entries", n, "backend", cfg.DedupBackend)
}
