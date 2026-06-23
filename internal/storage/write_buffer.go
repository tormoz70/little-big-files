package storage

import (
	"sync"
	"time"
)

type pendingWrite struct {
	record []byte
	loc    Location
	done   chan error
}

// WriteBuffer batches segment appends; one fsync per flush.
type WriteBuffer struct {
	maxSize     int
	maxInterval time.Duration

	mu      sync.Mutex
	pending []*pendingWrite
	bufSize int

	sm *SegmentManager
}

func NewWriteBuffer(sm *SegmentManager, maxSize int, maxInterval time.Duration) *WriteBuffer {
	wb := &WriteBuffer{
		maxSize:     maxSize,
		maxInterval: maxInterval,
		sm:          sm,
	}
	go wb.flushLoop()
	return wb
}

func (wb *WriteBuffer) Append(record []byte) (Location, error) {
	pw := &pendingWrite{
		record: record,
		done:   make(chan error, 1),
	}

	wb.mu.Lock()
	wb.pending = append(wb.pending, pw)
	wb.bufSize += len(record)
	shouldFlush := wb.bufSize >= wb.maxSize
	wb.mu.Unlock()

	if shouldFlush {
		wb.Flush()
	}

	if err := <-pw.done; err != nil {
		return Location{}, err
	}
	return pw.loc, nil
}

func (wb *WriteBuffer) flushLoop() {
	ticker := time.NewTicker(wb.maxInterval)
	defer ticker.Stop()
	for range ticker.C {
		wb.Flush()
	}
}

func (wb *WriteBuffer) Flush() {
	wb.mu.Lock()
	if len(wb.pending) == 0 {
		wb.mu.Unlock()
		return
	}
	batch := wb.pending
	wb.pending = nil
	wb.bufSize = 0
	wb.mu.Unlock()

	locs, err := wb.sm.batchAppend(batch)
	for i, pw := range batch {
		if err != nil {
			pw.done <- err
		} else {
			pw.loc = locs[i]
			pw.done <- nil
		}
	}
}

func (wb *WriteBuffer) Close() {
	wb.Flush()
}
