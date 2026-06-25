package storage_test

import (
	"testing"

	"github.com/little-big-files/little-big-files/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeRecordHeader(t *testing.T) {
	payload := []byte("payload-bytes")
	record := storage.EncodeRecord(storage.MagicZIP, payload)
	magic, size, err := storage.DecodeRecordHeader(record)
	require.NoError(t, err)
	require.Equal(t, storage.MagicZIP, magic)
	require.Equal(t, uint32(len(payload)), size)
}

func TestDecodeRecordHeaderTooShort(t *testing.T) {
	_, _, err := storage.DecodeRecordHeader([]byte{1, 2})
	require.Error(t, err)
}

func TestRecordTypeMagic(t *testing.T) {
	require.Equal(t, storage.MagicXML, storage.RecordXML.Magic())
	require.Equal(t, storage.MagicZIP, storage.RecordZIP.Magic())
	require.Equal(t, storage.MagicERR, storage.RecordError.Magic())
	require.True(t, storage.IsCompressedXML(storage.MagicXMLC))
}

func TestFooterRoundTrip(t *testing.T) {
	footer := storage.EncodeFooter(42, 12345)
	rc, ts, ok := storage.ValidateFooter(append([]byte("data"), footer...))
	require.True(t, ok)
	require.Equal(t, uint32(42), rc)
	require.Equal(t, uint64(12345), ts)
}

func TestValidateFooterInvalid(t *testing.T) {
	_, _, ok := storage.ValidateFooter([]byte("short"))
	require.False(t, ok)
}
