//go:build !rocksdb

package dedup

import (
	"fmt"

	"github.com/little-big-files/little-big-files/internal/config"
)

func openRocksDBIndex(cfg config.Config) (*HotIndex, error) {
	return nil, fmt.Errorf("rocksdb backend requires build tag -tags rocksdb and librocksdb-dev")
}
