package main

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/beeleelee/gcp/cmd/progressbar"
	"github.com/beeleelee/gcp/logger"
)

func cpDirToHost(
	ctx context.Context,
	hostAddr, srcDir, target string,
	chunkSize int64,
	batch int,
	timeout time.Duration,
	maxRetries int,
	useChecksum bool,
) error {
	cc, err := newClient(ctx, hostAddr, batch, timeout, useChecksum)
	if err != nil {
		return err
	}

	return filepath.Walk(srcDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		remotePath := filepath.Join(target, rel)

		if info.IsDir() {
			logger.Log.Debug("creating remote directory", "path", remotePath)
			_, err = cc.Create(remotePath, 0, info.Mode().Perm())
			return err
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		logger.Log.Debug("uploading file", "src", path, "dst", remotePath)
		return uploadFile(ctx, cc, path, remotePath, info, chunkSize, batch, maxRetries)
	})
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
