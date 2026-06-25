package compress_test

import (
	"testing"

	"github.com/little-big-files/little-big-files/internal/compress"
	"github.com/stretchr/testify/require"
)

func TestSidecarSaveLoadRoundTrip(t *testing.T) {
	root := t.TempDir()
	sc := compress.NewSidecar(root)
	dict := []byte("fake-zstd-dictionary-bytes-for-test")

	require.NoError(t, sc.Save(1, dict))

	id, loaded, err := sc.LoadCurrent()
	require.NoError(t, err)
	require.Equal(t, 1, id)
	require.Equal(t, dict, loaded)
}

func TestSidecarLoadCurrentMissing(t *testing.T) {
	sc := compress.NewSidecar(t.TempDir())
	id, dict, err := sc.LoadCurrent()
	require.NoError(t, err)
	require.Equal(t, 0, id)
	require.Nil(t, dict)
}

func TestSidecarSaveEmptyNoOp(t *testing.T) {
	sc := compress.NewSidecar(t.TempDir())
	require.NoError(t, sc.Save(1, nil))
	id, dict, err := sc.LoadCurrent()
	require.NoError(t, err)
	require.Equal(t, 0, id)
	require.Nil(t, dict)
}
