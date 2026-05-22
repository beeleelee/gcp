package main

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/beeleelee/gcp/cmd/progressbar"
)

func cpOneFileToHost(
	ctx context.Context,
	hostAddr, src, target string,
	chunkSize int64,
	batch int,
) (err error) {
	// open the src
	sfd, err := os.Open(src)
	if err != nil {
		return
	}
	defer sfd.Close()
	// read file meta data
	sfinfo, err := sfd.Stat()
	if err != nil {
		return
	}
	cc := newClient(ctx, hostAddr, batch)

	// touch target file
	_, err = cc.Create(target, sfinfo.Size(), sfinfo.Mode())
	if err != nil {
		return
	}
	var wg sync.WaitGroup
	var offset int64
	remainSize := sfinfo.Size()

	concurrentCtl := make(chan struct{}, batch)
	errChan := make(chan error, batch)
	progressChan := make(chan int64, batch+1)
	// print progress
	go progressbar.Progress(ctx, sfinfo.Size(), progressChan, time.Now(), time.Millisecond*200)

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
			buf := make([]byte, size)
			_, err := sfd.ReadAt(buf, off)
			if err != nil {
				select {
				case errChan <- err:
				case <-ctx.Done():
				}
				return
			}
			_, err = cc.Write(target, off, buf)
			if err != nil {
				select {
				case errChan <- err:
				case <-ctx.Done():
				}
				return
			}
			select {
			case progressChan <- size:
			case <-ctx.Done():
			}
		}(offset, chus)
		offset += chus
		remainSize -= chus
	}
	wg.Wait()
	return
}
