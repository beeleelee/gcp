package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/beeleelee/gcp/asyncio"
)

// hasGlob checks whether the base name of a path contains any shell glob
// metacharacters (*, ?, [).
func hasGlob(s string) bool {
	return strings.ContainsAny(filepath.Base(s), "*?[")
}

// expandLocalSource resolves a local file path or glob pattern into a
// concrete list of file paths. If glob returns a directory and recursive
// is true, the directory is walked and all regular files are included.
func expandLocalSource(pattern string, recursive bool) ([]string, error) {
	if !hasGlob(pattern) {
		return []string{pattern}, nil
	}

	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("no files match %q", pattern)
	}

	seen := map[string]bool{}
	var result []string
	for _, m := range matches {
		if seen[m] {
			continue
		}
		seen[m] = true
		if recursive {
			info, err := os.Stat(m)
			if err == nil && info.IsDir() {
				filepath.Walk(m, func(walkPath string, walkInfo os.FileInfo, walkErr error) error {
					if walkErr != nil {
						return walkErr
					}
					if walkInfo.Mode().IsRegular() && !seen[walkPath] {
						seen[walkPath] = true
						result = append(result, walkPath)
					}
					return nil
				})
				continue
			}
		}
		result = append(result, m)
	}
	return result, nil
}

// expandRemoteSources resolves a remote glob pattern by listing the parent
// directory on the server and matching entries locally. Returns addresses
// in "host:port/path" format suitable for the remote address parser.
func expandRemoteSources(
	ctx context.Context,
	hostPort, remotePath string,
	timeout time.Duration,
	useChecksum bool,
) ([]string, error) {
	if !hasGlob(remotePath) {
		return []string{hostPort + remotePath}, nil
	}

	parent, pattern := path.Split(remotePath)
	if parent == "" {
		parent = "/"
	}

	cc, err := newClient(ctx, hostPort, 1, timeout, useChecksum)
	if err != nil {
		return nil, err
	}
	defer cc.Close()

	res, err := cc.ReadDir(parent)
	if err != nil {
		return nil, err
	}
	dirRes := res.msg.(*asyncio.ReadDirRes)
	if !dirRes.Success {
		return nil, fmt.Errorf("readdir failed for %s", parent)
	}

	var result []string
	for _, entry := range dirRes.Entries {
		ok, _ := path.Match(pattern, entry.Name)
		if ok {
			result = append(result, hostPort+path.Join(parent, entry.Name))
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no files match pattern %q in %s", pattern, parent)
	}
	return result, nil
}
