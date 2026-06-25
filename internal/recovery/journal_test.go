package recovery_test

import (
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"github.com/little-big-files/little-big-files/internal/metadata"
	"github.com/little-big-files/little-big-files/internal/recovery"
	"github.com/stretchr/testify/require"
)

func TestJournalAppendReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	j, err := recovery.NewJournal(dir)
	require.NoError(t, err)
	defer j.Close()

	name := "ekb_2447_20250102.zip"
	entry := recovery.JournalEntry{
		PackageID:   42,
		SupplierID:  1577,
		ReceivedAt:  recovery.FormatTimeUTC(time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC)),
		PackageHash: hex.EncodeToString([]byte("package-hash-bytes-32-chars-long!!")),
		PayloadType: "zip",
		StorageMode: "zip_with_members",
		FileCount:   2,
		Files: []recovery.FileRef{
			{FileID: 1, BlobHash: hex.EncodeToString([]byte("blob-a-32-bytes-long-padding!!")), Role: "original", OriginalFilename: &name},
		},
	}
	require.NoError(t, j.Append(entry))

	loaded, err := recovery.ReadJournal(filepath.Join(dir, recovery.JournalFile))
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	require.Equal(t, entry.PackageID, loaded[0].PackageID)
	require.Equal(t, entry.SupplierID, loaded[0].SupplierID)
	require.Equal(t, entry.PayloadType, loaded[0].PayloadType)
	require.Len(t, loaded[0].Files, 1)
	require.Equal(t, int64(1), loaded[0].Files[0].FileID)
}

func TestEntryFromPackage(t *testing.T) {
	fn := "test.zip"
	hash := []byte("0123456789abcdef0123456789abcdef")
	pkg := &metadata.Package{
		ID:           7,
		SupplierID:   2447,
		ReceivedAt:   time.Date(2025, 6, 1, 10, 0, 0, 0, time.UTC),
		PackageHash:  hash,
		PayloadType:  "zip",
		StorageMode:  "single",
		FileCount:    1,
		OriginalFilename: &fn,
		Files: []metadata.PackageFile{
			{ID: 99, PackageID: 7, BlobHash: hash, Role: "original", OriginalFilename: &fn},
		},
	}
	e := recovery.EntryFromPackage(pkg)
	require.Equal(t, int64(7), e.PackageID)
	require.Equal(t, 2447, e.SupplierID)
	require.Equal(t, hex.EncodeToString(hash), e.PackageHash)
	require.Len(t, e.Files, 1)
	require.Equal(t, int64(99), e.Files[0].FileID)
}
