package compress_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/little-big-files/little-big-files/internal/compress"
	"github.com/little-big-files/little-big-files/internal/config"
	"github.com/stretchr/testify/require"
)

type dictRepoStub struct {
	dict []byte
	save bool
}

func (d *dictRepoStub) GetLatestDictionary(ctx context.Context) ([]byte, int, error) {
	return d.dict, len(d.dict), nil
}

func (d *dictRepoStub) SaveDictionary(ctx context.Context, dict []byte, entryCount int) error {
	d.dict = dict
	d.save = true
	return nil
}

func TestBootstrapEncoderDisabled(t *testing.T) {
	enc, err := compress.BootstrapEncoder(context.Background(), config.Config{CompressionEnabled: false}, &dictRepoStub{})
	require.NoError(t, err)
	require.Nil(t, enc)
}

func TestBootstrapEncoderWithExistingDict(t *testing.T) {
	samples := bootstrapTrainingSamples(200)
	dict, err := compress.TrainDictionary(samples, compress.DefaultDictSize)
	require.NoError(t, err)

	repo := &dictRepoStub{dict: dict}
	enc, err := compress.BootstrapEncoder(context.Background(), config.Config{
		CompressionEnabled: true,
		CompressionMinSize: 32,
		DataDir:            t.TempDir(),
	}, repo)
	require.NoError(t, err)
	require.NotNil(t, enc)
	defer enc.Close()
}

func TestBootstrapEncoderMirrorsDictToSidecar(t *testing.T) {
	// Repetitive payloads so TrainDictionary produces a non-empty dict in CI.
	var samples [][]byte
	chunk := []byte(strings.Repeat(`<a id="`, 100))
	for i := 0; i < 300; i++ {
		samples = append(samples, append(append([]byte{}, chunk...), byte('0'+i%10)))
	}
	dict, err := compress.TrainDictionary(samples, compress.DefaultDictSize)
	require.NoError(t, err)
	if len(dict) == 0 {
		t.Skip("TrainDictionary produced no dictionary")
	}

	dataRoot := t.TempDir()
	repo := &dictRepoStub{dict: dict}
	enc, err := compress.BootstrapEncoder(context.Background(), config.Config{
		CompressionEnabled: true,
		CompressionMinSize: 32,
		DataDir:            filepath.Join(dataRoot, "segments"),
	}, repo)
	require.NoError(t, err)
	require.NotNil(t, enc)
	defer enc.Close()

	sc := compress.NewSidecar(dataRoot)
	id, loaded, err := sc.LoadCurrent()
	require.NoError(t, err)
	require.Equal(t, 1, id)
	require.Equal(t, dict, loaded)
}

func bootstrapTrainingSamples(n int) [][]byte {
	base := `<?xml version="1.0"?><root><item id="%d">value-%d</item></root>`
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, []byte(fmt.Sprintf(base, i, i)))
	}
	return out
}

func TestBootstrapEncoderNoDictNoExamples(t *testing.T) {
	enc, err := compress.BootstrapEncoder(context.Background(), config.Config{
		CompressionEnabled: true,
		CompressionMinSize: 32,
		ExamplesDir:        t.TempDir(),
	}, &dictRepoStub{})
	require.NoError(t, err)
	require.NotNil(t, enc)
	defer enc.Close()
}

func TestEncoderShouldCompress(t *testing.T) {
	enc, err := compress.NewEncoder(nil, 64)
	require.NoError(t, err)
	defer enc.Close()
	require.False(t, enc.ShouldCompress(32))
	require.True(t, enc.ShouldCompress(64))
}

func TestEncoderNoCompressionWhenLarger(t *testing.T) {
	enc, err := compress.NewEncoder(nil, 1)
	require.NoError(t, err)
	defer enc.Close()

	tiny := []byte("x")
	out, err := enc.Compress(tiny)
	require.NoError(t, err)
	require.Equal(t, tiny, out)
}

func TestDecompressPlainPassthrough(t *testing.T) {
	enc, err := compress.NewEncoder(nil, 1)
	require.NoError(t, err)
	defer enc.Close()

	plain := []byte(`<?xml version="1.0"?><x/>`)
	out, err := enc.Decompress(plain)
	require.NoError(t, err)
	require.Equal(t, plain, out)
}

func TestTrainDictionaryEmpty(t *testing.T) {
	dict, err := compress.TrainDictionary(nil, compress.DefaultDictSize)
	require.NoError(t, err)
	require.Nil(t, dict)
}
