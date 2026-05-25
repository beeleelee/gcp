package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"time"

	"github.com/beeleelee/gcp/asyncio"
)

func downloadFile(
	ctx context.Context,
	cc *copierClient,
	src, target string,
	chunkSize int64,
	batch int,
	maxRetries int,
	useChecksum bool,
) error {
	tfd, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer tfd.Close()

	var (
		res     clientWrappedMsg
		statRes *asyncio.StatRes
		statErr error
	)
	for attempt := 0; attempt <= maxRetries; attempt++ {
		res, statErr = cc.Stat(src)
		if statErr == nil {
			statRes = res.msg.(*asyncio.StatRes)
			if !statRes.Success {
				statErr = fmt.Errorf("server returned success=false for stat")
			} else if statRes.IsDir {
				statErr = fmt.Errorf("path is a directory")
			} else {
				break
			}
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
	if statErr != nil {
		return statErr
	}

	fileSize := statRes.Size
	return processChunks(ctx, fileSize, chunkSize, batch,
		func(ctx context.Context, offset, size int64, progressChan chan<- int64) error {
			var (
				res     clientWrappedMsg
				readErr error
			)
		retryLoop:
			for attempt := 0; attempt <= maxRetries; attempt++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				res, readErr = cc.Read(src, offset, size)
				if readErr == nil {
					readRes := res.msg.(*asyncio.ReadRes)
					if !readRes.Success {
						readErr = fmt.Errorf("server returned success=false for offset %d", offset)
					} else if useChecksum && readRes.Checksum != 0 && crc32.ChecksumIEEE(res.payload) != readRes.Checksum {
						readErr = fmt.Errorf("checksum mismatch for offset %d", offset)
					} else {
						break retryLoop
					}
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
			if readErr != nil {
				return readErr
			}

			if _, err := tfd.WriteAt(res.payload, offset); err != nil {
				return err
			}
			select {
			case progressChan <- int64(len(res.payload)):
			case <-ctx.Done():
			}
			return nil
		},
	)
}

func cpOneFileFromHost(
	ctx context.Context,
	hostAddr, src, target string,
	chunkSize int64,
	batch int,
	timeout time.Duration,
	maxRetries int,
	useChecksum bool,
) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cc, err := newClient(ctx, hostAddr, batch, timeout, useChecksum)
	if err != nil {
		return
	}

	return downloadFile(ctx, cc, src, target, chunkSize, batch, maxRetries, useChecksum)
}
