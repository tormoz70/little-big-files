package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// UnpackEnqueued counts large-ZIP unpack jobs accepted into the queue.
	UnpackEnqueued = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lbf_unpack_enqueued_total",
		Help: "Large-ZIP async unpack jobs accepted into the queue",
	})
	// UnpackDropped counts jobs dropped because the queue was full. Dropped jobs
	// are re-discovered by the periodic recovery scan, so this is a backpressure
	// signal rather than data loss.
	UnpackDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lbf_unpack_dropped_total",
		Help: "Large-ZIP async unpack jobs dropped due to a full queue (retried later)",
	})
	// UnpackRecovered counts jobs re-enqueued by the periodic recovery scan of
	// packages still in raw_large state (after a restart or a prior drop).
	UnpackRecovered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lbf_unpack_recovered_total",
		Help: "Large-ZIP unpack jobs re-enqueued by the recovery scan",
	})
)
