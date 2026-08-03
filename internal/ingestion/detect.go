package ingestion

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
)

type PayloadType string

const (
	PayloadZIP PayloadType = "zip"
	PayloadXML PayloadType = "xml"
)

var ErrZipTooLarge = errors.New("zip unpacked size exceeds limit")

const DefaultMaxUnpackedZipBytes = 512 * 1024 * 1024

func effectiveMaxUnpackedZipBytes(maxUnpacked int64) int64 {
	if maxUnpacked > 0 {
		return maxUnpacked
	}
	return DefaultMaxUnpackedZipBytes
}

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

type ZipMemberFn func(filename string, data []byte) error

func zipReader(data []byte) (*zip.Reader, error) {
	return zip.NewReader(bytes.NewReader(data), int64(len(data)))
}

func shouldSkipZipEntry(name string, isDir bool) bool {
	return isDir || strings.Contains(name, "..")
}

func ForEachZipMember(data []byte, maxUnpacked int64, fn ZipMemberFn) error {
	maxUnpacked = effectiveMaxUnpackedZipBytes(maxUnpacked)
	r, err := zipReader(data)
	if err != nil {
		return err
	}
	var total int64
	for _, f := range r.File {
		if shouldSkipZipEntry(f.Name, f.FileInfo().IsDir()) {
			continue
		}
		uncompressed := int64(f.UncompressedSize64)
		if uncompressed > maxUnpacked || total+uncompressed > maxUnpacked {
			return fmt.Errorf("%w: limit %d", ErrZipTooLarge, maxUnpacked)
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		remaining := maxUnpacked - total
		body, err := io.ReadAll(io.LimitReader(rc, remaining+1))
		rc.Close()
		if err != nil {
			return err
		}
		if int64(len(body)) > remaining {
			return fmt.Errorf("%w: limit %d", ErrZipTooLarge, maxUnpacked)
		}
		total += int64(len(body))
		if err := fn(f.Name, body); err != nil {
			return err
		}
	}
	return nil
}

func UnpackZipWithLimit(data []byte, maxUnpacked int64) ([]ZipMember, error) {
	var members []ZipMember
	err := ForEachZipMember(data, maxUnpacked, func(name string, body []byte) error {
		members = append(members, ZipMember{Filename: name, Data: body})
		return nil
	})
	return members, err
}

func UnpackZip(data []byte) ([]ZipMember, error) {
	return UnpackZipWithLimit(data, DefaultMaxUnpackedZipBytes)
}

func CountZipEntries(data []byte) (int, error) {
	r, err := zipReader(data)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, f := range r.File {
		if !shouldSkipZipEntry(f.Name, f.FileInfo().IsDir()) {
			count++
		}
	}
	return count, nil
}

func CheckZipUncompressedLimit(data []byte, maxUnpacked int64) error {
	maxUnpacked = effectiveMaxUnpackedZipBytes(maxUnpacked)
	r, err := zipReader(data)
	if err != nil {
		return err
	}
	var total int64
	for _, f := range r.File {
		if shouldSkipZipEntry(f.Name, f.FileInfo().IsDir()) {
			continue
		}
		size := int64(f.UncompressedSize64)
		if size > maxUnpacked || total+size > maxUnpacked {
			return fmt.Errorf("%w: limit %d", ErrZipTooLarge, maxUnpacked)
		}
		total += size
	}
	return nil
}
