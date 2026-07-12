package diskspace

import (
	"errors"
	"fmt"
	"os"
)

func MinAvailableBytes(paths []string) (int64, error) {
	var (
		hasPath bool
		minFree int64
	)

	for _, p := range paths {
		if p == "" {
			continue
		}
		if _, err := os.Stat(p); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return 0, fmt.Errorf("stat path %q: %w", p, err)
		}
		free, err := availableBytes(p)
		if err != nil {
			return 0, fmt.Errorf("available bytes for %q: %w", p, err)
		}
		if !hasPath || free < minFree {
			minFree = free
			hasPath = true
		}
	}
	if !hasPath {
		return 0, fmt.Errorf("no existing paths provided")
	}
	return minFree, nil
}
