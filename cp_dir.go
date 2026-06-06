package main

import (
	"context"
	"os"
	"path/filepath"

	"github.com/beeleelee/gcp/logger"
)

// cpDirToHost recursively uploads a local directory tree to the remote
// host. It uses filepath.Walk to discover files, creates remote directory
// entries via cc.Create (size=0), and uploads each regular file via
// uploadFile.
func cpDirToHost(
	ctx context.Context,
	cc *copierClient,
	srcDir, target string,
	chunkSize int64,
	maxRetries int,
	useSha256 bool,
	compressionAlgo uint8,
	quiet bool,
) error {
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
		return uploadFile(ctx, cc, path, remotePath, info, chunkSize, cc.batch, maxRetries, useSha256, compressionAlgo, quiet)
	})
}
