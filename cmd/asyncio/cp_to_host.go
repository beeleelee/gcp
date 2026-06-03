package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/beeleelee/gcp/asyncio"
	"github.com/beeleelee/gcp/logger"
)

// cpOneFileToHost opens a local file and uploads it to the remote host using
// the already-connected shared client cc.
func cpOneFileToHost(
	ctx context.Context,
	cc *copierClient,
	src, target string,
	chunkSize int64,
	maxRetries int,
	useSha256 bool,
	compressionAlgo uint8,
) (err error) {
	sfinfo, err := os.Stat(src)
	if err != nil {
		return
	}
	return uploadFile(ctx, cc, src, target, sfinfo, chunkSize, cc.batch, maxRetries, useSha256, compressionAlgo)
}

// uploadFile orchestrates a single file upload to the remote host. It
// loads any existing resume state (creating the remote file only for fresh
// transfers), then dispatches concurrent chunk writes via processChunks.
// On completion it optionally verifies the file hash and deletes the
// resume state.
func uploadFile(
	ctx context.Context,
	cc *copierClient,
	srcPath, target string,
	info os.FileInfo,
	chunkSize int64,
	batch int,
	maxRetries int,
	useSha256 bool,
	compressionAlgo uint8,
) error {
	state := loadResumeState(srcPath, target, info.Size(), chunkSize, batch)

	if state == nil {
		sfd, err := os.Open(srcPath)
		if err != nil {
			return err
		}
		sfd.Close()

		res, err := cc.Create(target, info.Size(), info.Mode().Perm())
		if err != nil {
			return err
		}
		createRes := res.msg.(*asyncio.CreateRes)
		if !createRes.Success {
			return fmt.Errorf("remote create failed: %s", createRes.Error)
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
			if err := uploadFileChunk(ctx, cc, sfd, target, offset, size, maxRetries, compressionAlgo); err != nil {
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

// uploadFileChunk reads a chunk from the local file at offset off and
// sends it to the remote host via cc.Write(). It retries on failure up to
// maxRetries times with exponential backoff.
func uploadFileChunk(ctx context.Context, cc *copierClient, sfd *os.File, target string, off, size int64, maxRetries int, compressionAlgo uint8) error {
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
		res, wErr := cc.Write(target, off, buf, compressionAlgo)
		if wErr != nil {
			writeErr = wErr
		} else {
			writeRes := res.msg.(*asyncio.WriteRes)
			if !writeRes.Success {
				writeErr = fmt.Errorf("remote write failed at offset %d: %s", off, writeRes.Error)
			} else {
				return nil
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
	return writeErr
}
