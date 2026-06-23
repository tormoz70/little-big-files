package ingestion

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"strings"
)

type PayloadType string

const (
	PayloadZIP PayloadType = "zip"
	PayloadXML PayloadType = "xml"
)

func DetectPayload(data []byte) (PayloadType, error) {
	if len(data) < 4 {
		return "", fmt.Errorf("payload too small")
	}
	if data[0] == 'P' && data[1] == 'K' {
		return PayloadZIP, nil
	}
	trimmed := strings.TrimSpace(string(data))
	if strings.HasPrefix(trimmed, "<?xml") || strings.HasPrefix(trimmed, "<") {
		return PayloadXML, nil
	}
	return "", fmt.Errorf("unsupported payload type")
}

func IsZip(data []byte) bool {
	t, err := DetectPayload(data)
	return err == nil && t == PayloadZIP
}

type ZipMember struct {
	Filename string
	Data     []byte
}

func UnpackZip(data []byte) ([]ZipMember, error) {
	r, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	var members []ZipMember
	for _, f := range r.File {
		if f.FileInfo().IsDir() {
			continue
		}
		name := f.Name
		if strings.Contains(name, "..") {
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
		members = append(members, ZipMember{Filename: name, Data: body})
	}
	return members, nil
}

func CountZipEntries(data []byte) (int, error) {
	r, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return 0, err
	}
	count := 0
	for _, f := range r.File {
		if !f.FileInfo().IsDir() {
			count++
		}
	}
	return count, nil
}
