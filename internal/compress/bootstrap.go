package compress

import (
	"context"
	"log/slog"

	"github.com/little-big-files/little-big-files/internal/config"
)

type dictRepo interface {
	GetLatestDictionary(ctx context.Context) ([]byte, int, error)
	SaveDictionary(ctx context.Context, dict []byte, entryCount int) error
}

// BootstrapEncoder loads or trains a dictionary-backed encoder.
func BootstrapEncoder(ctx context.Context, cfg config.Config, repo dictRepo) (*Encoder, error) {
	if !cfg.CompressionEnabled {
		return nil, nil
	}

	dict, _, err := repo.GetLatestDictionary(ctx)
	if err != nil {
		return nil, err
	}
	if len(dict) == 0 {
		samples, err := LoadSamplesFromExamples(cfg.ExamplesDir, 500)
		if err != nil {
			slog.Warn("dictionary training skipped", "err", err)
		}
		if len(samples) > 0 {
			dict, err = TrainDictionary(samples, DefaultDictSize)
			if err != nil {
				return nil, err
			}
			if err := repo.SaveDictionary(ctx, dict, len(samples)); err != nil {
				return nil, err
			}
			slog.Info("trained compression dictionary", "samples", len(samples), "dict_bytes", len(dict))
		}
	}

	enc, err := NewEncoder(dict, cfg.CompressionMinSize)
	if err != nil {
		return nil, err
	}
	if len(dict) > 0 {
		slog.Info("compression enabled", "dict_bytes", len(dict))
	} else {
		slog.Info("compression enabled without dictionary")
	}
	return enc, nil
}
