package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/beeleelee/gcp/message"
	"github.com/beeleelee/gcp/logger"
)

// cpDirFromHost recursively downloads a remote directory tree to the
// local filesystem using the already-connected shared client cc.
func cpDirFromHost(
	ctx context.Context,
	cc *copierClient,
	srcDir, target string,
	chunkSize int64,
	maxRetries int,
	useSha256 bool,
	compressionAlgo uint8,
) error {
	return walkRemoteDir(ctx, cc, srcDir, target,
		chunkSize, cc.batch, maxRetries, cc.useChecksum, useSha256, compressionAlgo)
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
	compressionAlgo uint8,
) error {
	if err := os.MkdirAll(target, 0755); err != nil {
		return err
	}

	statRes, err := cc.Stat(srcDir)
	if err != nil {
		return err
	}
	sr := statRes.msg.(*message.StatRes)
	if sr.Success {
		if err := os.Chmod(target, os.FileMode(sr.Mode).Perm()); err != nil {
			return err
		}
	}

	res, err := cc.ReadDir(srcDir)
	if err != nil {
		return err
	}
	dirRes := res.msg.(*message.ReadDirRes)
	if !dirRes.Success {
		return fmt.Errorf("readdir failed for %s", srcDir)
	}

	for _, entry := range dirRes.Entries {
		remotePath := filepath.Join(srcDir, entry.Name)
		localPath := filepath.Join(target, entry.Name)

		if entry.IsDir {
			if err := walkRemoteDir(ctx, cc, remotePath, localPath,
				chunkSize, batch, maxRetries, useChecksum, useSha256, compressionAlgo); err != nil {
				return err
			}
		} else {
			logger.Log.Debug("downloading file", "src", remotePath, "dst", localPath)
			if err := downloadFile(ctx, cc, remotePath, localPath,
				chunkSize, batch, maxRetries, useChecksum, useSha256, compressionAlgo); err != nil {
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
