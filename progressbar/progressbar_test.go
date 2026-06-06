package progressbar

import (
	"context"
	"testing"
	"time"
)

func TestProgressCompletes(t *testing.T) {
	ctx := context.Background()
	total := int64(100)
	ch := make(chan int64, 10)

	done := make(chan struct{})
	go func() {
		Progress(ctx, total, ch, time.Now(), time.Millisecond*200)
		close(done)
	}()

	ch <- 50
	ch <- 30
	ch <- 20

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Progress did not complete after sending total bytes")
	}
}

func TestProgressCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan int64)

	done := make(chan struct{})
	go func() {
		Progress(ctx, 100, ch, time.Now(), time.Millisecond*200)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Progress did not return after context cancellation")
	}
}
