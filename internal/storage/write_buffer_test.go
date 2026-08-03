package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type panicBatchAppender struct{}

func (panicBatchAppender) batchAppend(batch []*pendingWrite) {
	panic("test panic")
}

type okBatchAppender struct {
	sm *SegmentManager
}

func (o okBatchAppender) batchAppend(batch []*pendingWrite) {
	o.sm.batchAppend(batch)
}

func TestWriteBufferFlushRecoversFromPanic(t *testing.T) {
	wb := NewWriteBuffer(panicBatchAppender{}, 1, time.Hour)
	defer wb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		wb.Append(ctx, []byte("test"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Append hung after batch panic")
	}
}

func TestWriteBufferFlushWithoutPanic(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSegmentManager(dir, 1024*1024)
	require.NoError(t, err)
	defer sm.Close()

	wb := NewWriteBuffer(okBatchAppender{sm: sm}, 1, time.Hour)
	sm.SetWriteBuffer(wb)
	defer wb.Close()

	ctx := context.Background()
	loc, err := wb.Append(ctx, []byte("payload"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, loc.Offset, int64(0))
}
