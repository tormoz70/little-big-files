package diskspace

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMinAvailableBytes(t *testing.T) {
	free, err := MinAvailableBytes([]string{t.TempDir()})
	require.NoError(t, err)
	require.Greater(t, free, int64(0))
}

func TestMinAvailableBytesSkipsMissingPaths(t *testing.T) {
	base := t.TempDir()
	missing := filepath.Join(base, "missing")
	free, err := MinAvailableBytes([]string{missing, base})
	require.NoError(t, err)
	require.Greater(t, free, int64(0))
}
