package compress

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// LoadSamplesFromExamples collects XML payloads from example ZIPs for dictionary training.
func LoadSamplesFromExamples(dir string, maxSamples int) ([][]byte, error) {
	var samples [][]byte
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".zip") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		xmls, err := extractXMLFromZip(data)
		if err != nil {
			return nil // skip bad zips
		}
		for _, x := range xmls {
			samples = append(samples, x)
			if maxSamples > 0 && len(samples) >= maxSamples {
				return io.EOF
			}
		}
		return nil
	})
	if err == io.EOF {
		return samples, nil
	}
	return samples, err
}

func extractXMLFromZip(data []byte) ([][]byte, error) {
	r, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	var out [][]byte
	for _, f := range r.File {
		if f.FileInfo().IsDir() {
			continue
		}
		if !strings.HasSuffix(strings.ToLower(f.Name), ".xml") {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		body, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, err
		}
		out = append(out, body)
	}
	return out, nil
}
