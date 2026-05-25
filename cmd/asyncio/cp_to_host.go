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
	timeout time.Duration,
	maxRetries int,
	useChecksum bool,
) (err error) {
	sfinfo, err := os.Stat(src)
	if err != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cc, err := newClient(ctx, hostAddr, batch, timeout, useChecksum)
	if err != nil {
		return
	}
	return uploadFile(ctx, cc, src, target, sfinfo, chunkSize, batch, maxRetries)
}

func uploadFile(
	ctx context.Context,
	cc *copierClient,
	srcPath, target string,
	info os.FileInfo,
	chunkSize int64,
	batch int,
	maxRetries int,
) error {
	sfd, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer sfd.Close()

	_, err = cc.Create(target, info.Size(), info.Mode().Perm())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var offset int64
	remainSize := info.Size()

	concurrentCtl := make(chan struct{}, batch)
	errChan := make(chan error, batch)
	progressChan := make(chan int64, batch+1)
	go progressbar.Progress(ctx, info.Size(), progressChan, time.Now(), time.Millisecond*200)

loop:
	for remainSize > 0 {
		select {
		case err = <-errChan:
			cancel()
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
			if err := uploadFileChunk(ctx, cc, sfd, target, off, size, maxRetries); err != nil {
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
	return nil
}

// uploadFileChunk reads a chunk from the local file and sends it via cc.Write with retries.
func uploadFileChunk(ctx context.Context, cc *copierClient, sfd *os.File, target string, off, size int64, maxRetries int) error {
	buf := make([]byte, size)
	if _, err := sfd.ReadAt(buf, off); err != nil {
		return err
	}
	var writeErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_, writeErr = cc.Write(target, off, buf)
		if writeErr == nil {
			return nil
		}
		if attempt < maxRetries {
			backoff := time.Duration(100<<attempt) * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return writeErr
}
