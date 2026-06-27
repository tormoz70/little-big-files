package ingestion

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/little-big-files/little-big-files/internal/metrics"
)

const (
	UnpackPending = "pending"

	defaultRecoverInterval = time.Minute
)

// UnpackQueue schedules background unpack of raw_large packages.
//
// Durability: the queue is in-memory, but it is reconciled against the metadata
// store. A package only leaves raw_large state when its unpack transaction
// commits, so any job dropped under backpressure or lost to a restart is
// rediscovered by the periodic recovery scan. An in-flight set deduplicates
// enqueues so a package is never unpacked twice concurrently.
type UnpackQueue struct {
	svc    *Service
	ch     chan int64
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	inflight map[int64]bool
}

func NewUnpackQueue(svc *Service, workers int, queueSize int) *UnpackQueue {
	if workers < 1 {
		workers = 1
	}
	if queueSize < 1 {
		queueSize = 64
	}
	ctx, cancel := context.WithCancel(context.Background())
	q := &UnpackQueue{
		svc:      svc,
		ch:       make(chan int64, queueSize),
		ctx:      ctx,
		cancel:   cancel,
		inflight: make(map[int64]bool),
	}
	for i := 0; i < workers; i++ {
		q.wg.Add(1)
		go q.worker(ctx, i)
	}
	return q
}

// StartRecovery launches a background loop that periodically re-enqueues any
// packages still stuck in raw_large state. Runs an initial scan shortly after
// start to recover work left pending by a previous process.
func (q *UnpackQueue) StartRecovery(interval time.Duration) {
	if interval <= 0 {
		interval = defaultRecoverInterval
	}
	q.wg.Add(1)
	go q.recoverLoop(q.ctx, interval)
}

func (q *UnpackQueue) Enqueue(packageID int64) {
	q.mu.Lock()
	if q.inflight[packageID] {
		q.mu.Unlock()
		return
	}
	q.inflight[packageID] = true
	q.mu.Unlock()

	select {
	case q.ch <- packageID:
		metrics.UnpackEnqueued.Inc()
	default:
		q.mu.Lock()
		delete(q.inflight, packageID)
		q.mu.Unlock()
		metrics.UnpackDropped.Inc()
		slog.Warn("unpack queue full, dropping job (will retry on next recovery scan)", "package_id", packageID)
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
			q.mu.Lock()
			delete(q.inflight, pkgID)
			q.mu.Unlock()
		}
	}
}

func (q *UnpackQueue) recoverLoop(ctx context.Context, interval time.Duration) {
	defer q.wg.Done()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			q.recoverPending(ctx)
			timer.Reset(interval)
		}
	}
}

func (q *UnpackQueue) recoverPending(ctx context.Context) {
	ids, err := q.svc.PendingLargePackageIDs(ctx)
	if err != nil {
		slog.Warn("unpack recovery scan failed", "err", err)
		return
	}
	for _, id := range ids {
		q.mu.Lock()
		already := q.inflight[id]
		q.mu.Unlock()
		if already {
			continue
		}
		metrics.UnpackRecovered.Inc()
		q.Enqueue(id)
	}
}

func (q *UnpackQueue) Shutdown() {
	q.cancel()
	q.wg.Wait()
}
