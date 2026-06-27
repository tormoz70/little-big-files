package storage_test

import (
	"context"
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
	loc, err := sm.Append(context.Background(), record)
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

func TestSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	// tiny max segment to force rotation
	sm, err := storage.NewSegmentManager(dir, 128)
	require.NoError(t, err)
	defer sm.Close()

	payload := []byte("012345678901234567890123456789012345678901234567890")
	record := storage.EncodeRecord(storage.MagicXML, payload)
	loc1, err := sm.Append(context.Background(), record)
	require.NoError(t, err)
	loc2, err := sm.Append(context.Background(), record)
	require.NoError(t, err)
	require.Equal(t, 0, loc1.SegmentID)
	require.Equal(t, 1, loc2.SegmentID)

	data, err := sm.Read(loc2)
	require.NoError(t, err)
	require.Equal(t, payload, data)
}

func TestSegmentReadRecordChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	sm, err := storage.NewSegmentManager(dir, 1024*1024)
	require.NoError(t, err)
	defer sm.Close()

	payload := []byte("corruptible payload bytes")
	record := storage.EncodeRecord(storage.MagicXML, payload)
	loc, err := sm.Append(context.Background(), record)
	require.NoError(t, err)

	// Simulate on-disk bit rot beneath the running manager (read handle is
	// opened lazily on ReadRecord and picks up the corrupted file).
	files, _ := os.ReadDir(dir)
	require.Len(t, files, 1)
	path := filepath.Join(dir, files[0].Name())
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	raw[storage.HeaderSize+3] ^= 0xFF
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	_, _, err = sm.ReadRecord(loc.SegmentID, loc.Offset)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum")
}

func TestSegmentReadRecordMagic(t *testing.T) {
	dir := t.TempDir()
	sm, err := storage.NewSegmentManager(dir, 1024*1024)
	require.NoError(t, err)
	defer sm.Close()

	payload := []byte("zip-data")
	record := storage.EncodeRecord(storage.MagicZIP, payload)
	loc, err := sm.Append(context.Background(), record)
	require.NoError(t, err)

	magic, data, err := sm.ReadRecord(loc.SegmentID, loc.Offset)
	require.NoError(t, err)
	require.Equal(t, storage.MagicZIP, magic)
	require.Equal(t, payload, data)
}
