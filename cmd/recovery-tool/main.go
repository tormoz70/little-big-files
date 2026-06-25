package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/dedup"
	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/little-big-files/little-big-files/internal/recovery"
)

func main() {
	apply := flag.Bool("apply", false, "write rebuilt metadata to PostgreSQL")
	flag.Parse()
	cfg := config.Load()
	ctx := context.Background()

	repo, err := metadata.NewPostgresRepository(ctx, cfg.PGDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer repo.Close()

	opt := recovery.RebuildOptions{
		DataDir:  cfg.DataDir,
		DataRoot: recovery.DataRootFromDataDir(cfg.DataDir),
		Apply:    *apply,
	}
	if err := recovery.Rebuild(ctx, repo, opt); err != nil {
		log.Fatal(err)
	}
	if *apply {
		idx, err := dedup.Open(cfg)
		if err != nil {
			log.Fatal(err)
		}
		if idx != nil {
			defer idx.Close()
			if err := dedup.RebuildFromPG(ctx, idx, repo, cfg.BloomExpectedItems, cfg.BloomFalsePositive); err != nil {
				log.Fatal(err)
			}
		}
		fmt.Println("dedup index rebuilt")
	}
}
