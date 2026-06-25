package recovery

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const journalVersion = 1
const JournalFile = "ingest_journal.ndjson"

type FileRef struct {
	FileID           int64   `json:"file_id"`
	BlobHash         string  `json:"blob_hash"`
	Role             string  `json:"role"`
	OriginalFilename *string `json:"original_filename,omitempty"`
	SequenceNumber   *int    `json:"sequence_number,omitempty"`
}

type JournalEntry struct {
	Version            int       `json:"v"`
	PackageID          int64     `json:"package_id"`
	SupplierID         int       `json:"supplier_id"`
	ReceivedAt         string    `json:"received_at"`
	PackageHash        string    `json:"package_hash"`
	PayloadType        string    `json:"payload_type"`
	StorageMode        string    `json:"storage_mode"`
	CanonicalPackageID *int64    `json:"canonical_package_id,omitempty"`
	OriginalFilename   *string   `json:"original_filename,omitempty"`
	FileCount          int       `json:"file_count"`
	UnpackError        *string   `json:"unpack_error,omitempty"`
	Files              []FileRef `json:"files"`
}

type Journal struct {
	path string
	mu   sync.Mutex
	f    *os.File
}

func NewJournal(dataDir string) (*Journal, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dataDir, JournalFile)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	return &Journal{path: path, f: f}, nil
}

func (j *Journal) Append(e JournalEntry) error {
	if e.Version == 0 {
		e.Version = journalVersion
	}
	line, err := json.Marshal(e)
	if err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err := j.f.Write(line); err != nil {
		return err
	}
	if _, err := j.f.Write([]byte("\n")); err != nil {
		return err
	}
	return j.f.Sync()
}

func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil {
		return nil
	}
	err := j.f.Close()
	j.f = nil
	return err
}

func ReadJournal(path string) ([]JournalEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []JournalEntry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e JournalEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("parse journal line: %w", err)
		}
		out = append(out, e)
	}
	return out, sc.Err()
}

func FormatTimeUTC(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
