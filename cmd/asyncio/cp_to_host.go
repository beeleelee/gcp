package main

import (
	"context"
	"os"
	"time"

	"github.com/beeleelee/gcp/logger"
)

func cpOneFileToHost(
	ctx context.Context,
	hostAddr, src, target string,
	chunkSize int64,
	batch int,
	timeout time.Duration,
	maxRetries int,
	useChecksum bool,
	useSha256 bool,
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
	return uploadFile(ctx, cc, src, target, sfinfo, chunkSize, batch, maxRetries, useSha256)
}

func uploadFile(
	ctx context.Context,
	cc *copierClient,
	srcPath, target string,
	info os.FileInfo,
	chunkSize int64,
	batch int,
	maxRetries int,
	useSha256 bool,
) error {
	state := loadResumeState(srcPath, target, info.Size(), chunkSize, batch)

	if state == nil {
		sfd, err := os.Open(srcPath)
		if err != nil {
			return err
		}
		sfd.Close()

		_, err = cc.Create(target, info.Size(), info.Mode().Perm())
		if err != nil {
			return err
		}
		logger.Log.Debug("upload: fresh transfer, state not found")
	} else {
		logger.Log.Debug("upload: resuming transfer", "completed", len(state.Completed))
	}

	sfd, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer sfd.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err = processChunks(ctx, info.Size(), chunkSize, 0, batch, state,
		func(ctx context.Context, offset, size int64, progressChan chan<- int64) error {
			if err := uploadFileChunk(ctx, cc, sfd, target, offset, size, maxRetries); err != nil {
				return err
			}
			select {
			case progressChan <- size:
			case <-ctx.Done():
			}
			if err := addCompletedOffset(state, offset); err != nil {
				logger.Log.Debug("upload: failed to save resume state", "err", err)
			}
			return nil
		},
	)
	if err != nil {
		return err
	}

	if useSha256 {
		if err := verifyFileHash(ctx, cc, target, srcPath); err != nil {
			return err
		}
	}

	deleteResumeState(state)
	return nil
}

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
