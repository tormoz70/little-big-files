package storage

import (
	"context"
	"sync"
	"time"
)

type pendingWrite struct {
	record []byte
	loc    Location
	err    error
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

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func NewWriteBuffer(sm *SegmentManager, maxSize int, maxInterval time.Duration) *WriteBuffer {
	wb := &WriteBuffer{
		maxSize:     maxSize,
		maxInterval: maxInterval,
		sm:          sm,
		stop:        make(chan struct{}),
	}
	wb.wg.Add(1)
	go wb.flushLoop()
	return wb
}

// Append enqueues a record and blocks until it is durably written (or the batch
// fails). It honours ctx cancellation: if the caller's context is cancelled
// before the flush completes, it returns ctx.Err(). The record may still be
// written by a later flush (a harmless orphan in the append-only segment).
func (wb *WriteBuffer) Append(ctx context.Context, record []byte) (Location, error) {
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

	select {
	case err := <-pw.done:
		if err != nil {
			return Location{}, err
		}
		return pw.loc, nil
	case <-ctx.Done():
		return Location{}, ctx.Err()
	}
}

func (wb *WriteBuffer) flushLoop() {
	defer wb.wg.Done()
	ticker := time.NewTicker(wb.maxInterval)
	defer ticker.Stop()
	for {
		select {
		case <-wb.stop:
			return
		case <-ticker.C:
			wb.Flush()
		}
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

	// batchAppend records per-write success/failure on each pendingWrite, so a
	// single bad record no longer fails unrelated writes sharing the batch.
	wb.sm.batchAppend(batch)
	for _, pw := range batch {
		pw.done <- pw.err
	}
}

func (wb *WriteBuffer) Close() {
	wb.stopOnce.Do(func() { close(wb.stop) })
	wb.wg.Wait()
	wb.Flush()
}
