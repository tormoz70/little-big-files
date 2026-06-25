#!/usr/bin/env python3
"""One-shot generator for recovery implementation files."""
import os
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def w(rel, content):
    path = os.path.join(ROOT, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(content.lstrip("\n"))
    print("wrote", rel)

FILES = {}

FILES["internal/storage/segment_index.go"] = r'''
package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

const (
	IndexMagic     uint32 = 0x49445846 // "IDXF"
	IndexVersion   uint32 = 1
	IndexEntrySize        = 60
	IndexFooterSize       = 32
)

type IndexEntry struct {
	Offset      int64
	StoredSize  uint32
	LogicalSize uint32
	Magic       uint32
	Hash        [32]byte
	SupplierID  uint32
	DictID      uint32
}

type SegmentIndex struct {
	dir   string
	mu    sync.Mutex
	files map[int]*os.File
}

func NewSegmentIndex(dataDir string) *SegmentIndex {
	return &SegmentIndex{dir: dataDir, files: make(map[int]*os.File)}
}

func (si *SegmentIndex) indexPath(segmentID int) string {
	return filepath.Join(si.dir, fmt.Sprintf("segment_%04d.idx", segmentID))
}

func EncodeIndexEntry(e IndexEntry) []byte {
	buf := make([]byte, IndexEntrySize)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(e.Offset))
	binary.LittleEndian.PutUint32(buf[8:12], e.StoredSize)
	binary.LittleEndian.PutUint32(buf[12:16], e.LogicalSize)
	binary.LittleEndian.PutUint32(buf[16:20], e.Magic)
	copy(buf[20:52], e.Hash[:])
	binary.LittleEndian.PutUint32(buf[52:56], e.SupplierID)
	binary.LittleEndian.PutUint32(buf[56:60], e.DictID)
	return buf
}

func DecodeIndexEntry(buf []byte) (IndexEntry, error) {
	if len(buf) < IndexEntrySize {
		return IndexEntry{}, fmt.Errorf("index entry too short")
	}
	var e IndexEntry
	e.Offset = int64(binary.LittleEndian.Uint64(buf[0:8]))
	e.StoredSize = binary.LittleEndian.Uint32(buf[8:12])
	e.LogicalSize = binary.LittleEndian.Uint32(buf[12:16])
	e.Magic = binary.LittleEndian.Uint32(buf[16:20])
	copy(e.Hash[:], buf[20:52])
	e.SupplierID = binary.LittleEndian.Uint32(buf[52:56])
	e.DictID = binary.LittleEndian.Uint32(buf[56:60])
	return e, nil
}

func encodeIndexFooter(recordCount uint64, lastOffset int64, entriesCRC uint32) []byte {
	buf := make([]byte, IndexFooterSize)
	binary.LittleEndian.PutUint32(buf[0:4], IndexVersion)
	binary.LittleEndian.PutUint64(buf[4:12], recordCount)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(lastOffset))
	binary.LittleEndian.PutUint32(buf[20:24], entriesCRC)
	binary.LittleEndian.PutUint32(buf[28:32], IndexMagic)
	return buf
}

func decodeIndexFooter(data []byte) (recordCount uint64, lastOffset int64, entriesCRC uint32, ok bool) {
	if len(data) < IndexFooterSize {
		return 0, 0, 0, false
	}
	off := len(data) - IndexFooterSize
	if binary.LittleEndian.Uint32(data[off+28:off+32]) != IndexMagic {
		return 0, 0, 0, false
	}
	if binary.LittleEndian.Uint32(data[off:off+4]) != IndexVersion {
		return 0, 0, 0, false
	}
	recordCount = binary.LittleEndian.Uint64(data[off+4 : off+12])
	lastOffset = int64(binary.LittleEndian.Uint64(data[off+12 : off+20]))
	entriesCRC = binary.LittleEndian.Uint32(data[off+20 : off+24])
	return recordCount, lastOffset, entriesCRC, true
}

func (si *SegmentIndex) Append(segmentID int, e IndexEntry) error {
	si.mu.Lock()
	defer si.mu.Unlock()

	f, err := si.openAppend(segmentID)
	if err != nil {
		return err
	}

	if _, err := f.Write(EncodeIndexEntry(e)); err != nil {
		return fmt.Errorf("write index entry: %w", err)
	}

	data, err := os.ReadFile(si.indexPath(segmentID))
	if err != nil {
		return err
	}
	entries := data
	if len(data) >= IndexFooterSize {
		if _, _, _, ok := decodeIndexFooter(data); ok {
			entries = data[:len(data)-IndexFooterSize]
		}
	}
	footer := encodeIndexFooter(uint64(len(entries)/IndexEntrySize), e.Offset, crc32.ChecksumIEEE(entries))
	if _, err := f.Write(footer); err != nil {
		return fmt.Errorf("write index footer: %w", err)
	}
	return f.Sync()
}

func (si *SegmentIndex) openAppend(segmentID int) (*os.File, error) {
	if f, ok := si.files[segmentID]; ok {
		return f, nil
	}
	path := si.indexPath(segmentID)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if info.Size() >= IndexFooterSize {
		data, err := os.ReadFile(path)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		if _, _, _, ok := decodeIndexFooter(data); ok {
			trunc := data[:len(data)-IndexFooterSize]
			if err := os.WriteFile(path, trunc, 0o644); err != nil {
				_ = f.Close()
				return nil, err
			}
			_ = f.Close()
			f, err = os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
			if err != nil {
				return nil, err
			}
		}
	}
	si.files[segmentID] = f
	return f, nil
}

func ReadIndexFile(path string) ([]IndexEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	entriesData := data
	var recordCount uint64
	var entriesCRC uint32
	if len(data) >= IndexFooterSize {
		var ok bool
		recordCount, _, entriesCRC, ok = decodeIndexFooter(data)
		if ok {
			entriesData = data[:len(data)-IndexFooterSize]
		}
	}
	if len(entriesData)%IndexEntrySize != 0 {
		return nil, fmt.Errorf("corrupt index file %s", path)
	}
	if entriesCRC != 0 && crc32.ChecksumIEEE(entriesData) != entriesCRC {
		return nil, fmt.Errorf("index CRC mismatch in %s", path)
	}
	n := len(entriesData) / IndexEntrySize
	if recordCount != 0 && uint64(n) != recordCount {
		return nil, fmt.Errorf("index record count mismatch in %s", path)
	}
	out := make([]IndexEntry, 0, n)
	for i := 0; i < n; i++ {
		off := i * IndexEntrySize
		e, err := DecodeIndexEntry(entriesData[off : off+IndexEntrySize])
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

func ListIndexFiles(dataDir string) ([]string, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var id int
		if _, err := fmt.Sscanf(e.Name(), "segment_%04d.idx", &id); err == nil {
			paths = append(paths, filepath.Join(dataDir, e.Name()))
		}
	}
	return paths, nil
}

func (si *SegmentIndex) Close() error {
	si.mu.Lock()
	defer si.mu.Unlock()
	for id, f := range si.files {
		_ = f.Close()
		delete(si.files, id)
	}
	return nil
}
'''

for rel, content in FILES.items():
    w(rel, content)
