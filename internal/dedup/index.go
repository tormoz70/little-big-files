package dedup

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/little-big-files/little-big-files/internal/metadata"
)

// HotIndex combines Bloom filter with a persistent/in-memory KV store.
type HotIndex struct {
	backend string
	bloom   *bloomFilter
	store   kvStore
}

func (idx *HotIndex) Backend() string { return idx.backend }

func (idx *HotIndex) MightContain(hash []byte) bool {
	if idx == nil || idx.bloom == nil {
		return true
	}
	return idx.bloom.MightContain(hash)
}

func (idx *HotIndex) Lookup(hash []byte) (Entry, bool) {
	if idx == nil || idx.store == nil {
		return Entry{}, false
	}
	e, ok, err := idx.store.Get(hash)
	if err != nil || !ok {
		return Entry{}, false
	}
	return e, true
}

func (idx *HotIndex) Put(hash []byte, entry Entry) error {
	if idx == nil || idx.store == nil {
		return nil
	}
	if err := idx.store.Put(hash, entry); err != nil {
		return err
	}
	if idx.bloom != nil {
		idx.bloom.Add(hash)
	}
	return nil
}

func (idx *HotIndex) Close() error {
	if idx == nil || idx.store == nil {
		return nil
	}
	return idx.store.Close()
}

func (idx *HotIndex) Len() (int, error) {
	if idx == nil || idx.store == nil {
		return 0, nil
	}
	return idx.store.Len()
}

// Open creates a dedup hot index for the configured backend.
func Open(cfg config.Config) (*HotIndex, error) {
	switch cfg.DedupBackend {
	case "", "postgres":
		return nil, nil
	case "memory":
		return openMemoryIndex(cfg.BloomExpectedItems, cfg.BloomFalsePositive)
	case "rocksdb":
		return openRocksDBIndex(cfg)
	default:
		return nil, fmt.Errorf("unknown dedup backend %q", cfg.DedupBackend)
	}
}

type blobLister interface {
	ListContentBlobs(ctx context.Context) ([]metadata.ContentBlob, error)
}

// RebuildFromPG repopulates Bloom + KV from content_blobs.
func RebuildFromPG(ctx context.Context, idx *HotIndex, repo blobLister, expectedItems uint, falsePositiveRate float64) error {
	if idx == nil {
		return nil
	}
	blobs, err := repo.ListContentBlobs(ctx)
	if err != nil {
		return err
	}
	if idx.store != nil {
		if err := idx.store.Clear(); err != nil {
			return err
		}
	}
	if idx.bloom != nil {
		n := expectedItems
		if len(blobs) > int(n) {
			n = uint(len(blobs))
		}
		idx.bloom.Reset(n, falsePositiveRate)
	}
	for _, b := range blobs {
		if err := idx.Put(b.ContentHash, Entry{
			SegmentID: b.SegmentID,
			Offset:    b.Offset,
			Size:      b.Size,
		}); err != nil {
			return err
		}
	}
	slog.Info("dedup index rebuilt", "backend", idx.backend, "blobs", len(blobs))
	return nil
}
