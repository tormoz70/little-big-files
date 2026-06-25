#!/usr/bin/env python3
import os, re
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def w(rel, content):
    path = os.path.join(ROOT, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8', newline='\n') as f:
        f.write(content.lstrip('\n'))

def patch(rel, old, new, count=1):
    path = os.path.join(ROOT, rel)
    text = open(path, encoding='utf-8').read()
    if old not in text:
        raise SystemExit(f'patch miss in {rel}: {old[:80]!r}')
    text = text.replace(old, new, count)
    with open(path, 'w', encoding='utf-8', newline='\n') as f:
        f.write(text)
    print('patched', rel)

# --- new files ---
w('internal/recovery/journal.go', r'''
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
''')

w('internal/recovery/journal_record.go', r'''
package recovery

import (
	"encoding/hex"

	"github.com/little-big-files/little-big-files/internal/metadata"
)

func EntryFromPackage(pkg *metadata.Package) JournalEntry {
	e := JournalEntry{
		PackageID:          pkg.ID,
		SupplierID:         pkg.SupplierID,
		ReceivedAt:         FormatTimeUTC(pkg.ReceivedAt),
		PackageHash:        hex.EncodeToString(pkg.PackageHash),
		PayloadType:        pkg.PayloadType,
		StorageMode:        pkg.StorageMode,
		CanonicalPackageID: pkg.CanonicalPackageID,
		OriginalFilename:   pkg.OriginalFilename,
		FileCount:          pkg.FileCount,
		UnpackError:        pkg.UnpackError,
	}
	for _, f := range pkg.Files {
		e.Files = append(e.Files, FileRef{
			FileID:           f.ID,
			BlobHash:         hex.EncodeToString(f.BlobHash),
			Role:             f.Role,
			OriginalFilename: f.OriginalFilename,
			SequenceNumber:   f.SequenceNumber,
		})
	}
	return e
}
''')

w('internal/compress/sidecar.go', open(os.path.join(ROOT,'internal/compress/sidecar.go'), encoding='utf-8').read() if os.path.exists(os.path.join(ROOT,'internal/compress/sidecar.go')) else r'''
package compress

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Sidecar struct{ dir string }

type currentMeta struct {
	DictID   int    `json:"dict_id"`
	SHA256   string `json:"sha256"`
	Filename string `json:"filename"`
}

func NewSidecar(dataRoot string) *Sidecar {
	return &Sidecar{dir: filepath.Join(dataRoot, "dictionaries")}
}

func dictFilename(dictID int, sha string) string {
	short := sha
	if len(short) > 16 {
		short = short[:16]
	}
	return fmt.Sprintf("dict_%04d_%s.zdict", dictID, short)
}

func (s *Sidecar) Save(dictID int, dict []byte) error {
	if len(dict) == 0 {
		return nil
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	sum := sha256.Sum256(dict)
	sha := hex.EncodeToString(sum[:])
	name := dictFilename(dictID, sha)
	final := filepath.Join(s.dir, name)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, dict, 0o644); err != nil {
		return err
	}
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	meta := currentMeta{DictID: dictID, SHA256: sha, Filename: name}
	mb, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	curTmp := filepath.Join(s.dir, "current.json.tmp")
	cur := filepath.Join(s.dir, "current.json")
	if err := os.WriteFile(curTmp, mb, 0o644); err != nil {
		return err
	}
	return os.Rename(curTmp, cur)
}

func (s *Sidecar) LoadCurrent() (dictID int, dict []byte, err error) {
	cur := filepath.Join(s.dir, "current.json")
	mb, err := os.ReadFile(cur)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil, nil
		}
		return 0, nil, err
	}
	var meta currentMeta
	if err := json.Unmarshal(mb, &meta); err != nil {
		return 0, nil, err
	}
	path := filepath.Join(s.dir, meta.Filename)
	dict, err = os.ReadFile(path)
	if err != nil {
		return 0, nil, err
	}
	sum := sha256.Sum256(dict)
	if hex.EncodeToString(sum[:]) != meta.SHA256 {
		return 0, nil, fmt.Errorf("dictionary checksum mismatch")
	}
	return meta.DictID, dict, nil
}
''')

print('phase1 done')
