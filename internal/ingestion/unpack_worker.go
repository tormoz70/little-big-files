package ingestion

import (
	"context"
	"log/slog"
	"sync"
)

const (
	UnpackPending = "pending"
)

// UnpackQueue schedules background unpack of raw_large packages.
type UnpackQueue struct {
	svc    *Service
	ch     chan int64
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func NewUnpackQueue(svc *Service, workers int, queueSize int) *UnpackQueue {
	if workers < 1 {
		workers = 1
	}
	if queueSize < 1 {
		queueSize = 64
	}
	ctx, cancel := context.WithCancel(context.Background())
	q := &UnpackQueue{svc: svc, ch: make(chan int64, queueSize), cancel: cancel}
	for i := 0; i < workers; i++ {
		q.wg.Add(1)
		go q.worker(ctx, i)
	}
	return q
}

func (q *UnpackQueue) Enqueue(packageID int64) {
	select {
	case q.ch <- packageID:
	default:
		slog.Warn("unpack queue full, dropping job", "package_id", packageID)
	}
}

func (q *UnpackQueue) worker(ctx context.Context, id int) {
	defer q.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case pkgID := <-q.ch:
			if err := q.svc.UnpackLargePackage(ctx, pkgID); err != nil {
				slog.Error("async large zip unpack failed", "package_id", pkgID, "err", err)
			} else {
				slog.Info("async large zip unpack done", "package_id", pkgID)
			}
		}
	}
}

func (q *UnpackQueue) Shutdown() {
	q.cancel()
	q.wg.Wait()
}
