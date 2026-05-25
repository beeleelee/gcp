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

func hasGlob(s string) bool {
	return strings.ContainsAny(filepath.Base(s), "*?[")
}

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
