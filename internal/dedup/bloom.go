package dedup

import (
	"sync"

	bloom "github.com/bits-and-blooms/bloom/v3"
)

type bloomFilter struct {
	mu  sync.RWMutex
	f   *bloom.BloomFilter
	cap uint
}

func newBloom(expectedItems uint, falsePositiveRate float64) *bloomFilter {
	if expectedItems == 0 {
		expectedItems = 1_000_000
	}
	if falsePositiveRate <= 0 {
		falsePositiveRate = 0.001
	}
	return &bloomFilter{
		f:   bloom.NewWithEstimates(expectedItems, falsePositiveRate),
		cap: expectedItems,
	}
}

func (b *bloomFilter) MightContain(hash []byte) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.f.Test(hash)
}

func (b *bloomFilter) Add(hash []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.f.Add(hash)
}

func (b *bloomFilter) Reset(expectedItems uint, falsePositiveRate float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if expectedItems == 0 {
		expectedItems = b.cap
	}
	if falsePositiveRate <= 0 {
		falsePositiveRate = 0.001
	}
	b.f = bloom.NewWithEstimates(expectedItems, falsePositiveRate)
	b.cap = expectedItems
}
