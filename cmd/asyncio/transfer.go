package main

import (
	"context"
	"sync"
	"time"

	"github.com/beeleelee/gcp/cmd/progressbar"
)

func processChunks(
	ctx context.Context,
	fileSize, chunkSize int64,
	batch int,
	fn func(context.Context, int64, int64, chan<- int64) error,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	sem := make(chan struct{}, batch)
	errChan := make(chan error, batch)
	progressChan := make(chan int64, batch+1)
	go progressbar.Progress(ctx, fileSize, progressChan, time.Now(), time.Millisecond*200)
	progressChan <- 0

	var offset int64
	remainSize := fileSize

	for remainSize > 0 {
		select {
		case err := <-errChan:
			cancel()
			return err
		default:
		}
		size := chunkSize
		if remainSize < size {
			size = remainSize
		}
		wg.Add(1)
		go func(off, sz int64) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()

			if err := fn(ctx, off, sz, progressChan); err != nil {
				select {
				case errChan <- err:
				case <-ctx.Done():
				}
			}
		}(offset, size)
		offset += size
		remainSize -= size
	}
	wg.Wait()

	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}
