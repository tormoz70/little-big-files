package compress_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/little-big-files/little-big-files/internal/compress"
	"github.com/stretchr/testify/require"
)

func trainingSamples(n int) [][]byte {
	base := `<?xml version="1.0"?><root><item id="%d">value-%d</item></root>`
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, []byte(fmt.Sprintf(base, i, i)))
	}
	return out
}

func TestEncoderRoundTrip(t *testing.T) {
	samples := trainingSamples(200)
	dict, err := compress.TrainDictionary(samples, compress.DefaultDictSize)
	require.NoError(t, err)

	enc, err := compress.NewEncoder(dict, 32)
	require.NoError(t, err)
	defer enc.Close()

	original := samples[0]
	compressed, err := enc.Compress(original)
	require.NoError(t, err)
	if len(dict) > 0 {
		require.Less(t, len(compressed), len(original))
	}

	restored, err := enc.Decompress(compressed)
	require.NoError(t, err)
	require.True(t, bytes.Equal(original, restored))
}

func TestTrainFromExamplesDir(t *testing.T) {
	samples, err := compress.LoadSamplesFromExamples("../../examples", 500)
	require.NoError(t, err)
	if len(samples) == 0 {
		t.Skip("no example zips")
	}
	dict, err := compress.TrainDictionary(samples, compress.DefaultDictSize)
	require.NoError(t, err)
	t.Logf("trained dict bytes: %d from %d samples", len(dict), len(samples))
}
