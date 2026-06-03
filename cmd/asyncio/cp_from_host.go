package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"time"

	"github.com/beeleelee/gcp/asyncio"
	"github.com/beeleelee/gcp/logger"
)

// downloadFile orchestrates a single file download from the remote host.
// It Stats the remote file for size, loads resume state, opens the local
// file (O_TRUNC for fresh, O_RDWR for resume), then dispatches concurrent
// chunk reads via processChunks. Each chunk is verified with CRC-32
// checksum if enabled. On completion it optionally verifies the file hash
// and deletes the resume state.
func downloadFile(
	ctx context.Context,
	cc *copierClient,
	src, target string,
	chunkSize int64,
	batch int,
	maxRetries int,
	useChecksum bool,
	useSha256 bool,
) error {
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
	state := loadResumeState(src, target, fileSize, chunkSize, batch)

	var tfd *os.File
	if state == nil {
		var err error
		tfd, err = os.OpenFile(target, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(statRes.Mode).Perm())
		if err != nil {
			return err
		}
		logger.Log.Debug("download: fresh transfer, state not found")
	} else {
		var err error
		tfd, err = os.OpenFile(target, os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		logger.Log.Debug("download: resuming transfer", "completed", len(state.Completed))
	}
	defer tfd.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err := processChunks(ctx, fileSize, chunkSize, 0, batch, state,
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
			if err := addCompletedOffset(state, offset); err != nil {
				logger.Log.Debug("download: failed to save resume state", "err", err)
			}
			return nil
		},
	)
	if err != nil {
		return err
	}

	if err := os.Chmod(target, os.FileMode(statRes.Mode).Perm()); err != nil {
		return err
	}

	if useSha256 {
		if err := verifyFileHash(ctx, cc, src, target); err != nil {
			return err
		}
	}

	deleteResumeState(state)
	return nil
}

// cpOneFileFromHost opens a connection and downloads a single file from
// the remote host to a local path.
func cpOneFileFromHost(
	ctx context.Context,
	hostAddr, src, target string,
	chunkSize int64,
	batch int,
	timeout time.Duration,
	maxRetries int,
	useChecksum bool,
	useSha256 bool,
) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cc, err := newClient(ctx, hostAddr, batch, timeout, useChecksum)
	if err != nil {
		return
	}

	return downloadFile(ctx, cc, src, target, chunkSize, batch, maxRetries, useChecksum, useSha256)
}
