package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/little-big-files/little-big-files/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestSegmentIndexRoundTrip(t *testing.T) {
	dir := t.TempDir()
	idx := storage.NewSegmentIndex(dir)
	defer idx.Close()

	var hash [32]byte
	hash[0] = 0xab
	entry := storage.IndexEntry{
		Offset: 128, StoredSize: 200, LogicalSize: 180,
		Magic: storage.MagicZIP, Hash: hash, SupplierID: 1577, DictID: 1,
	}
	require.NoError(t, idx.Append(0, entry))

	path := filepath.Join(dir, "segment_0000.idx")
	entries, err := storage.ReadIndexFile(path)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, entry.Offset, entries[0].Offset)
	require.Equal(t, entry.SupplierID, entries[0].SupplierID)
}

func TestSegmentIndexMultipleEntriesAndList(t *testing.T) {
	dir := t.TempDir()
	idx := storage.NewSegmentIndex(dir)
	defer idx.Close()

	var h1, h2 [32]byte
	h1[0], h2[0] = 1, 2
	require.NoError(t, idx.Append(0, storage.IndexEntry{Offset: 0, StoredSize: 50, LogicalSize: 40, Magic: storage.MagicXML, Hash: h1, SupplierID: 2447}))
	require.NoError(t, idx.Append(0, storage.IndexEntry{Offset: 50, StoredSize: 60, LogicalSize: 55, Magic: storage.MagicZIP, Hash: h2, SupplierID: 1577}))

	paths, err := storage.ListIndexFiles(dir)
	require.NoError(t, err)
	require.Len(t, paths, 1)

	entries, err := storage.ReadIndexFile(paths[0])
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, uint32(1577), entries[1].SupplierID)
}

func TestReadIndexFileCRCMismatch(t *testing.T) {
	dir := t.TempDir()
	idx := storage.NewSegmentIndex(dir)
	defer idx.Close()

	var hash [32]byte
	require.NoError(t, idx.Append(0, storage.IndexEntry{Offset: 0, StoredSize: 10, LogicalSize: 8, Magic: storage.MagicXML, Hash: hash}))

	path := filepath.Join(dir, "segment_0000.idx")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	// Corrupt one byte in the entries region.
	data[0] ^= 0xff
	require.NoError(t, os.WriteFile(path, data, 0o644))

	_, err = storage.ReadIndexFile(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CRC")
}
