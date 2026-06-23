package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/little-big-files/little-big-files/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestSegmentAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	sm, err := storage.NewSegmentManager(dir, 1024*1024)
	require.NoError(t, err)
	defer sm.Close()

	payload := []byte("hello xml")
	record := storage.EncodeRecord(storage.MagicXML, payload)
	loc, err := sm.Append(record)
	require.NoError(t, err)

	data, err := sm.Read(loc)
	require.NoError(t, err)
	require.Equal(t, payload, data)

	// recovery
	require.NoError(t, sm.Close())
	sm2, err := storage.NewSegmentManager(dir, 1024*1024)
	require.NoError(t, err)
	defer sm2.Close()

	data2, err := sm2.Read(loc)
	require.NoError(t, err)
	require.Equal(t, payload, data2)

	files, _ := os.ReadDir(dir)
	require.Len(t, files, 1)
	require.Contains(t, files[0].Name(), "segment_")
	_ = filepath.Base(dir)
}

func TestContentHashDeterministic(t *testing.T) {
	h1 := storage.ContentHash([]byte("same"))
	h2 := storage.ContentHash([]byte("same"))
	require.Equal(t, h1, h2)
	require.Len(t, h1, 32)
}
