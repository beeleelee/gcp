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

// cpDirFromHost recursively downloads a remote directory tree to the
// local filesystem. It creates a client connection then delegates to
// walkRemoteDir.
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

// walkRemoteDir recursively reads a remote directory listing and downloads
// each entry. Directories are created locally; regular files are
// downloaded via downloadFile. This is the counterpart of filepath.Walk
// for the remote filesystem protocol.
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

	statRes, err := cc.Stat(srcDir)
	if err != nil {
		return err
	}
	sr := statRes.msg.(*asyncio.StatRes)
	if sr.Success {
		if err := os.Chmod(target, os.FileMode(sr.Mode).Perm()); err != nil {
			return err
		}
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
			if entry.Mode != 0 {
				if err := os.Chmod(localPath, os.FileMode(entry.Mode).Perm()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
