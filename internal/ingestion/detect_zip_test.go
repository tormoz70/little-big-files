package ingestion_test

import (
	"archive/zip"
	"bytes"
	"errors"
	"testing"

	"github.com/little-big-files/little-big-files/internal/ingestion"
	"github.com/stretchr/testify/require"
)

func TestCheckZipUncompressedLimitRejectsOversize(t *testing.T) {
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, err := w.Create("big.xml")
	require.NoError(t, err)
	_, err = f.Write(bytes.Repeat([]byte("x"), 1024))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	err = ingestion.CheckZipUncompressedLimit(buf.Bytes(), 512)
	require.Error(t, err)
	require.True(t, errors.Is(err, ingestion.ErrZipTooLarge))
}

func TestForEachZipMemberStreamsMembers(t *testing.T) {
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for i := 0; i < 3; i++ {
		name := string(rune('a' + i))
		f, err := w.Create(name + ".xml")
		require.NoError(t, err)
		_, err = f.Write([]byte("<x/>"))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	count := 0
	err := ingestion.ForEachZipMember(buf.Bytes(), 1024*1024, func(name string, data []byte) error {
		count++
		require.NotEmpty(t, name)
		require.NotEmpty(t, data)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

func TestForEachZipMemberSkipsPathTraversal(t *testing.T) {
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	evil, err := w.CreateHeader(&zip.FileHeader{Name: "../../evil.xml"})
	require.NoError(t, err)
	_, err = evil.Write([]byte("<bad/>"))
	require.NoError(t, err)
	safe, err := w.Create("safe.xml")
	require.NoError(t, err)
	_, err = safe.Write([]byte("<ok/>"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	var names []string
	err = ingestion.ForEachZipMember(buf.Bytes(), 1024, func(name string, data []byte) error {
		names = append(names, name)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"safe.xml"}, names)

	count, err := ingestion.CountZipEntries(buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, 1, count)
}
