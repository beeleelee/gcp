package main

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/beeleelee/gcp/asyncio"
	"github.com/beeleelee/gcp/cmd/progressbar"
)

func cpOneFileFromHost(
	ctx context.Context,
	hostAddr, src, target string,
	chunkSize int64,
	batch int,
) (err error) {
	tfd, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	defer tfd.Close()

	cc := newClient(ctx, hostAddr, batch)

	// first read to get file size
	res, err := cc.Read(src, 0, chunkSize)
	if err != nil {
		return
	}
	readRes := res.msg.(*asyncio.ReadRes)
	if !readRes.Success {
		return
	}
	_, err = tfd.WriteAt(res.payload, 0)
	if err != nil {
		return
	}

	fileSize := readRes.FileSize
	var offset int64 = int64(len(res.payload))
	remainSize := fileSize - offset

	var wg sync.WaitGroup
	concurrentCtl := make(chan struct{}, batch)
	errChan := make(chan error, batch)
	progressChan := make(chan int64, batch+1)
	go progressbar.Progress(ctx, fileSize, progressChan, time.Now(), time.Millisecond*200)
	progressChan <- int64(len(res.payload))

loop:
	for remainSize > 0 {
		select {
		case err = <-errChan:
			break loop
		default:
		}
		chus := chunkSize
		if remainSize < chus {
			chus = remainSize
		}
		wg.Add(1)
		go func(off int64, size int64) {
			defer wg.Done()
			select {
			case concurrentCtl <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-concurrentCtl }()
			res, err := cc.Read(src, off, size)
			if err != nil {
				select {
				case errChan <- err:
				case <-ctx.Done():
				}
				return
			}
			readRes := res.msg.(*asyncio.ReadRes)
			if !readRes.Success {
				select {
				case errChan <- context.Canceled:
				case <-ctx.Done():
				}
				return
			}
			_, err = tfd.WriteAt(res.payload, off)
			if err != nil {
				select {
				case errChan <- err:
				case <-ctx.Done():
				}
				return
			}
			select {
			case progressChan <- int64(len(res.payload)):
			case <-ctx.Done():
			}
		}(offset, chus)
		offset += chus
		remainSize -= chus
	}
	wg.Wait()
	return
}
