package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Location struct {
	SegmentID int
	Offset    int64
	Size      int // on-disk record size (header + payload)
}

type SegmentManager struct {
	dir            string
	maxSegmentSize int64
	verifyChecksum bool
	mu             sync.Mutex
	activeID       int
	activeFile     *os.File
	activeOffset   int64
	recordCount    uint32
	buffer         *WriteBuffer

	readMu    sync.Mutex
	readFiles map[int]*os.File
	readMeta  map[int]readHandleMeta
}

type readHandleMeta struct {
	size    int64
	modTime time.Time
}

func NewSegmentManager(dir string, maxSegmentSize int64) (*SegmentManager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create segment dir: %w", err)
	}
	sm := &SegmentManager{
		dir:            dir,
		maxSegmentSize: maxSegmentSize,
		verifyChecksum: true,
		readFiles:      make(map[int]*os.File),
		readMeta:       make(map[int]readHandleMeta),
	}
	if err := sm.recover(); err != nil {
		return nil, err
	}
	return sm, nil
}

func (sm *SegmentManager) SetWriteBuffer(wb *WriteBuffer) {
	sm.buffer = wb
}

// SetVerifyChecksum toggles per-record CRC verification on the read path.
// The checksum trailer is always written regardless of this setting.
func (sm *SegmentManager) SetVerifyChecksum(v bool) {
	sm.verifyChecksum = v
}

func (sm *SegmentManager) recover() error {
	entries, err := os.ReadDir(sm.dir)
	if err != nil {
		return fmt.Errorf("read segment dir: %w", err)
	}
	maxID := -1
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var id int
		if _, err := fmt.Sscanf(e.Name(), "segment_%04d.dat", &id); err != nil {
			continue
		}
		if id > maxID {
			maxID = id
		}
	}
	if maxID < 0 {
		return sm.openSegment(0)
	}
	sm.activeID = maxID
	return sm.openSegmentFile(maxID)
}

func truncateToValid(data []byte) ([]byte, uint32, int64) {
	var offset int64
	var count uint32
	for {
		if len(data)-int(offset) < HeaderSize {
			break
		}
		magic, size, err := DecodeRecordHeader(data[offset:])
		if err != nil || size == 0 || !KnownMagic(magic) {
			break
		}
		recordEnd := offset + int64(HeaderSize) + int64(size) + int64(ChecksumSize)
		if recordEnd > int64(len(data)) {
			break
		}
		payload := data[offset+int64(HeaderSize) : offset+int64(HeaderSize)+int64(size)]
		storedCRC := binary.LittleEndian.Uint32(data[offset+int64(HeaderSize)+int64(size) : recordEnd])
		if RecordChecksum(payload) != storedCRC {
			break
		}
		count++
		offset = recordEnd
	}
	if len(data)-int(offset) >= FooterSize {
		if _, _, ok := ValidateFooter(data[:offset+FooterSize]); ok {
			offset += FooterSize
		}
	}
	return data[:offset], count, offset
}

func (sm *SegmentManager) segmentPath(id int) string {
	return filepath.Join(sm.dir, fmt.Sprintf("segment_%04d.dat", id))
}

func (sm *SegmentManager) openSegment(id int) error {
	sm.activeID = id
	sm.activeOffset = 0
	sm.recordCount = 0
	return sm.openSegmentFile(id)
}

func (sm *SegmentManager) openSegmentFile(id int) error {
	if sm.activeFile != nil {
		_ = sm.activeFile.Close()
	}
	path := sm.segmentPath(id)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	sm.activeFile = f
	if info.Size() == 0 {
		sm.activeOffset = 0
		sm.recordCount = 0
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		_ = f.Close()
		return err
	}
	truncated, rc, off := truncateToValid(data)
	if int64(len(truncated)) < info.Size() {
		if err := os.WriteFile(path, truncated, 0o644); err != nil {
			_ = f.Close()
			return err
		}
		_ = f.Close()
		f, err = os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
		sm.activeFile = f
	}
	sm.activeOffset = off
	sm.recordCount = rc
	return nil
}

func (sm *SegmentManager) Append(ctx context.Context, record []byte) (Location, error) {
	if sm.buffer != nil {
		return sm.buffer.Append(ctx, record)
	}
	return sm.appendDirect(record)
}

func (sm *SegmentManager) appendDirect(record []byte) (Location, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.appendLocked(record, true)
}

// batchAppend writes every record in the batch and records the per-record
// outcome (loc/err) on each pendingWrite, then issues a single fsync. A failure
// on one record does not abort the others, so unrelated requests sharing a batch
// are not failed together. A failed fsync marks all otherwise-successful records
// in the batch as failed (their durability is not guaranteed).
func (sm *SegmentManager) batchAppend(batch []*pendingWrite) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	wrote := false
	for _, pw := range batch {
		loc, err := sm.appendLocked(pw.record, false)
		if err != nil {
			pw.err = err
			continue
		}
		pw.loc = loc
		wrote = true
	}
	if wrote && sm.activeFile != nil {
		if err := sm.activeFile.Sync(); err != nil {
			syncErr := fmt.Errorf("batch fsync: %w", err)
			for _, pw := range batch {
				if pw.err == nil {
					pw.err = syncErr
				}
			}
		}
	}
}

func (sm *SegmentManager) appendLocked(record []byte, fsync bool) (Location, error) {
	if sm.activeFile == nil {
		return Location{}, fmt.Errorf("no active segment")
	}

	recordSize := int64(len(record))
	if sm.activeOffset > 0 && sm.activeOffset+recordSize+FooterSize > sm.maxSegmentSize {
		if err := sm.finalizeLocked(); err != nil {
			return Location{}, err
		}
		if err := sm.openSegment(sm.activeID + 1); err != nil {
			return Location{}, err
		}
	}

	loc := Location{
		SegmentID: sm.activeID,
		Offset:    sm.activeOffset,
		Size:      len(record),
	}

	n, err := sm.activeFile.WriteAt(record, sm.activeOffset)
	if err != nil {
		return Location{}, fmt.Errorf("segment write: %w", err)
	}
	if n != len(record) {
		return Location{}, fmt.Errorf("short write")
	}
	if fsync {
		if err := sm.activeFile.Sync(); err != nil {
			return Location{}, fmt.Errorf("segment fsync: %w", err)
		}
	}

	sm.activeOffset += recordSize
	sm.recordCount++
	return loc, nil
}

func (sm *SegmentManager) finalizeLocked() error {
	if sm.activeFile == nil || sm.activeOffset == 0 {
		return nil
	}
	footer := EncodeFooter(sm.recordCount, uint64(sm.activeOffset))
	if _, err := sm.activeFile.WriteAt(footer, sm.activeOffset); err != nil {
		return err
	}
	return sm.activeFile.Sync()
}

func (sm *SegmentManager) openReadHandle(segmentID int) (*os.File, error) {
	sm.readMu.Lock()
	defer sm.readMu.Unlock()

	path := sm.segmentPath(segmentID)
	info, err := os.Stat(path)
	if err != nil {
		if old, ok := sm.readFiles[segmentID]; ok {
			_ = old.Close()
			delete(sm.readFiles, segmentID)
			delete(sm.readMeta, segmentID)
		}
		return nil, err
	}

	if f, ok := sm.readFiles[segmentID]; ok {
		meta := sm.readMeta[segmentID]
		if meta.size == info.Size() && meta.modTime.Equal(info.ModTime()) {
			return f, nil
		}
		_ = f.Close()
		delete(sm.readFiles, segmentID)
		delete(sm.readMeta, segmentID)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	sm.readFiles[segmentID] = f
	sm.readMeta[segmentID] = readHandleMeta{
		size:    info.Size(),
		modTime: info.ModTime(),
	}
	return f, nil
}

// ReadRecord reads a segment record and returns magic + payload bytes.
func (sm *SegmentManager) ReadRecord(segmentID int, offset int64) (magic uint32, payload []byte, err error) {
	f, err := sm.openReadHandle(segmentID)
	if err != nil {
		return 0, nil, err
	}

	hdr := make([]byte, HeaderSize)
	n, err := f.ReadAt(hdr, offset)
	if err != nil {
		return 0, nil, err
	}
	if n != HeaderSize {
		return 0, nil, fmt.Errorf("short header read")
	}
	magic, size, err := DecodeRecordHeader(hdr)
	if err != nil {
		return 0, nil, err
	}

	payload = make([]byte, size)
	n, err = f.ReadAt(payload, offset+HeaderSize)
	if err != nil {
		return 0, nil, err
	}
	if int(n) != int(size) {
		return 0, nil, fmt.Errorf("short payload read")
	}

	if sm.verifyChecksum {
		trailer := make([]byte, ChecksumSize)
		tn, terr := f.ReadAt(trailer, offset+HeaderSize+int64(size))
		if terr != nil || tn != ChecksumSize {
			return 0, nil, fmt.Errorf("read record checksum: %w", terr)
		}
		want := binary.LittleEndian.Uint32(trailer)
		if RecordChecksum(payload) != want {
			return 0, nil, fmt.Errorf("record checksum mismatch in segment %d at offset %d", segmentID, offset)
		}
	}
	return magic, payload, nil
}

// Read is kept for tests; returns payload only.
func (sm *SegmentManager) Read(loc Location) ([]byte, error) {
	_, payload, err := sm.ReadRecord(loc.SegmentID, loc.Offset)
	return payload, err
}

// TotalBytes returns on-disk size of all segment files.
func (sm *SegmentManager) TotalBytes() (int64, error) {
	entries, err := os.ReadDir(sm.dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

func (sm *SegmentManager) SegmentDir() string { return sm.dir }

func (sm *SegmentManager) Close() error {
	if sm.buffer != nil {
		sm.buffer.Close()
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if err := sm.finalizeLocked(); err != nil {
		return err
	}
	if sm.activeFile != nil {
		_ = sm.activeFile.Close()
	}
	sm.readMu.Lock()
	for _, f := range sm.readFiles {
		_ = f.Close()
	}
	sm.readMu.Unlock()
	return nil
}
