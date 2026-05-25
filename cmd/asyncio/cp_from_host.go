package main

import (
	"context"
	"fmt"
	"hash/crc32"
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
	timeout time.Duration,
	maxRetries int,
	useChecksum bool,
) (err error) {
	tfd, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	defer tfd.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cc, err := newClient(ctx, hostAddr, batch, timeout, useChecksum)
	if err != nil {
		return
	}

	// get remote file size with stat (with retries)
	var (
		res     clientWrappedMsg
		statRes *asyncio.StatRes
	)
	for attempt := 0; attempt <= maxRetries; attempt++ {
		res, err = cc.Stat(src)
		if err == nil {
			statRes = res.msg.(*asyncio.StatRes)
			if !statRes.Success {
				err = fmt.Errorf("server returned success=false for stat")
			} else if statRes.IsDir {
				err = fmt.Errorf("path is a directory")
			} else {
				break
			}
		}
		if attempt < maxRetries {
			backoff := time.Duration(100<<attempt) * time.Millisecond
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
	}
	if err != nil {
		return
	}

	fileSize := statRes.Size
	var offset int64
	remainSize := fileSize

	var wg sync.WaitGroup
	concurrentCtl := make(chan struct{}, batch)
	errChan := make(chan error, batch)
	progressChan := make(chan int64, batch+1)
	go progressbar.Progress(ctx, fileSize, progressChan, time.Now(), time.Millisecond*200)
	progressChan <- 0

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

			var (
				res     clientWrappedMsg
				readErr error
			)
		retryLoop:
			for attempt := 0; attempt <= maxRetries; attempt++ {
				select {
				case <-ctx.Done():
					errChan <- ctx.Err()
					return
				default:
				}
				res, readErr = cc.Read(src, off, size)
				if readErr == nil {
					readRes := res.msg.(*asyncio.ReadRes)
					if !readRes.Success {
						readErr = fmt.Errorf("server returned success=false for offset %d", off)
					} else if useChecksum && readRes.Checksum != 0 && crc32.ChecksumIEEE(res.payload) != readRes.Checksum {
						readErr = fmt.Errorf("checksum mismatch for offset %d", off)
					} else {
						break retryLoop
					}
				}
				if attempt < maxRetries {
					backoff := time.Duration(100<<attempt) * time.Millisecond
					select {
					case <-ctx.Done():
						errChan <- ctx.Err()
						return
					case <-time.After(backoff):
					}
				}
			}
			if readErr != nil {
				select {
				case errChan <- readErr:
				case <-ctx.Done():
				}
				return
			}

			_, err := tfd.WriteAt(res.payload, off)
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
