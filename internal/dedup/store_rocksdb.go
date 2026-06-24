//go:build rocksdb

package dedup

import (
	"fmt"

	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/linxGnu/grocksdb"
)

type rocksDBStore struct {
	db *grocksdb.DB
}

func openRocksDBIndex(cfg config.Config) (*HotIndex, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	defer opts.Destroy()

	db, err := grocksdb.OpenDb(opts, cfg.RocksDBPath)
	if err != nil {
		return nil, fmt.Errorf("open rocksdb: %w", err)
	}
	return &HotIndex{
		backend: "rocksdb",
		bloom:   newBloom(cfg.BloomExpectedItems, cfg.BloomFalsePositive),
		store:   &rocksDBStore{db: db},
	}, nil
}

func (s *rocksDBStore) Get(hash []byte) (Entry, bool, error) {
	val, err := s.db.GetBytes(hash)
	if err != nil {
		return Entry{}, false, err
	}
	if val == nil {
		return Entry{}, false, nil
	}
	e, err := decodeEntry(val)
	if err != nil {
		return Entry{}, false, err
	}
	return e, true, nil
}

func (s *rocksDBStore) Put(hash []byte, entry Entry) error {
	return s.db.Put(hash, encodeEntry(entry))
}

func (s *rocksDBStore) Clear() error {
	it := s.db.NewIterator(grocksdb.NewDefaultReadOptions())
	defer it.Close()
	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		wb.Delete(it.Key().Copy())
	}
	return s.db.Write(grocksdb.NewDefaultWriteOptions(), wb)
}

func (s *rocksDBStore) Close() error {
	s.db.Close()
	return nil
}

func (s *rocksDBStore) Len() (int, error) {
	it := s.db.NewIterator(grocksdb.NewDefaultReadOptions())
	defer it.Close()
	n := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		n++
	}
	return n, nil
}
