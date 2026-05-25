package main

import (
	"context"
	"os"
	"time"
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

	return processChunks(ctx, info.Size(), chunkSize, batch,
		func(ctx context.Context, offset, size int64, progressChan chan<- int64) error {
			if err := uploadFileChunk(ctx, cc, sfd, target, offset, size, maxRetries); err != nil {
				return err
			}
			select {
			case progressChan <- size:
			case <-ctx.Done():
			}
			return nil
		},
	)
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
