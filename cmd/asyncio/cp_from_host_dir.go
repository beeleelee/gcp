package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/beeleelee/gcp/asyncio"
	"github.com/beeleelee/gcp/logger"
)

func cpDirFromHost(
	ctx context.Context,
	hostAddr, srcDir, target string,
	chunkSize int64,
	batch int,
	timeout time.Duration,
	maxRetries int,
	useChecksum bool,
	useSha256 bool,
) error {
	cc, err := newClient(ctx, hostAddr, batch, timeout, useChecksum)
	if err != nil {
		return err
	}
	defer cc.Close()

	return walkRemoteDir(ctx, cc, srcDir, target,
		chunkSize, batch, maxRetries, useChecksum, useSha256)
}

func walkRemoteDir(
	ctx context.Context,
	cc *copierClient,
	srcDir, target string,
	chunkSize int64,
	batch int,
	maxRetries int,
	useChecksum bool,
	useSha256 bool,
) error {
	if err := os.MkdirAll(target, 0755); err != nil {
		return err
	}

	res, err := cc.ReadDir(srcDir)
	if err != nil {
		return err
	}
	dirRes := res.msg.(*asyncio.ReadDirRes)
	if !dirRes.Success {
		return fmt.Errorf("readdir failed for %s", srcDir)
	}

	for _, entry := range dirRes.Entries {
		remotePath := filepath.Join(srcDir, entry.Name)
		localPath := filepath.Join(target, entry.Name)

		if entry.IsDir {
			if err := walkRemoteDir(ctx, cc, remotePath, localPath,
				chunkSize, batch, maxRetries, useChecksum, useSha256); err != nil {
				return err
			}
		} else {
			logger.Log.Debug("downloading file", "src", remotePath, "dst", localPath)
			if err := downloadFile(ctx, cc, remotePath, localPath,
				chunkSize, batch, maxRetries, useChecksum, useSha256); err != nil {
				return err
			}
		}
	}
	return nil
}
