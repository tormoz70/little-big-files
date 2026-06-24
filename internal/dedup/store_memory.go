package dedup

import (
	"sync"
)

type memoryStore struct {
	mu    sync.RWMutex
	items map[string]Entry
}

func newMemoryStore() *memoryStore {
	return &memoryStore{items: make(map[string]Entry)}
}

func hashKey(hash []byte) string { return string(hash) }

func (s *memoryStore) Get(hash []byte) (Entry, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.items[hashKey(hash)]
	return e, ok, nil
}

func (s *memoryStore) Put(hash []byte, entry Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[hashKey(hash)] = entry
	return nil
}

func (s *memoryStore) Close() error { return nil }

func (s *memoryStore) Len() (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.items), nil
}

func (s *memoryStore) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = make(map[string]Entry)
	return nil
}

func openMemoryIndex(expectedItems uint, falsePositiveRate float64) (*HotIndex, error) {
	return &HotIndex{
		backend: "memory",
		bloom:   newBloom(expectedItems, falsePositiveRate),
		store:   newMemoryStore(),
	}, nil
}
