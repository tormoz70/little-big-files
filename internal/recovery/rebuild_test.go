package recovery_test

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/little-big-files/little-big-files/internal/compress"
	"github.com/little-big-files/little-big-files/internal/recovery"
	"github.com/little-big-files/little-big-files/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestRebuildDryRunFromArtifacts(t *testing.T) {
	dataRoot := t.TempDir()
	dataDir := filepath.Join(dataRoot, "segments")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))

	var hash [32]byte
	copy(hash[:], []byte("dry-run-blob-hash-32-bytes!!!!!!"))
	idx := storage.NewSegmentIndex(dataDir)
	defer idx.Close()
	require.NoError(t, idx.Append(0, storage.IndexEntry{
		Offset: 0, StoredSize: 100, LogicalSize: 80,
		Magic: storage.MagicZIP, Hash: hash, SupplierID: 1577, DictID: 0,
	}))

	j, err := recovery.NewJournal(dataDir)
	require.NoError(t, err)
	require.NoError(t, j.Append(recovery.JournalEntry{
		PackageID: 1, SupplierID: 1577, PackageHash: hex.EncodeToString(hash[:]),
		PayloadType: "zip", StorageMode: "single", FileCount: 1,
	}))
	j.Close()

	dict := []byte("test-dictionary")
	require.NoError(t, compress.NewSidecar(dataRoot).Save(1, dict))

	err = recovery.Rebuild(context.Background(), nil, recovery.RebuildOptions{
		DataDir:  dataDir,
		DataRoot: dataRoot,
		Apply:    false,
	})
	require.NoError(t, err)
}
