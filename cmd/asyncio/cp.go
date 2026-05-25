package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/beeleelee/gcp/asyncio"
	"github.com/beeleelee/gcp/logger"
	"github.com/urfave/cli/v2"
)

func isRemoteAddr(s string) bool {
	return strings.Contains(s, ":")
}

func lookupHosts(hostname string) (string, error) {
	f, err := os.Open("/etc/hosts")
	if err != nil {
		return "", fmt.Errorf("cannot open /etc/hosts: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if idx := strings.Index(line, "#"); idx >= 0 {
			line = line[:idx]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		ip := fields[0]
		if strings.Contains(ip, ":") {
			continue
		}
		if net.ParseIP(ip) == nil {
			continue
		}
		for _, name := range fields[1:] {
			if name == hostname {
				return ip, nil
			}
		}
	}
	return "", fmt.Errorf("hostname %q not found in /etc/hosts", hostname)
}

func isRemoteDir(ctx context.Context, hostAddr, path string, timeout time.Duration, useChecksum bool) (bool, error) {
	cc, err := newClient(ctx, hostAddr, 1, timeout, useChecksum)
	if err != nil {
		return false, err
	}
	defer cc.Close()
	res, err := cc.Stat(path)
	if err != nil {
		return false, err
	}
	statRes := res.msg.(*asyncio.StatRes)
	if !statRes.Success {
		return false, fmt.Errorf("stat failed for %s", path)
	}
	return statRes.IsDir, nil
}

func parseRemoteAddr(s string) (hostPort, path string, err error) {
	colonIdx := strings.Index(s, ":")
	if colonIdx < 0 {
		return "", "", fmt.Errorf("invalid remote address %q: missing colon", s)
	}

	host := s[:colonIdx]
	if host == "" {
		return "", "", fmt.Errorf("invalid remote address %q: empty host", s)
	}

	rest := s[colonIdx+1:]

	var i int
	for i = 0; i < len(rest); i++ {
		if rest[i] < '0' || rest[i] > '9' {
			break
		}
	}

	port := "1717"
	path = rest

	if i > 0 {
		port = rest[:i]
		rem := rest[i:]
		switch {
		case len(rem) == 0:
			path = ""
		case rem[0] == '/':
			path = rem
		case rem[0] == ':':
			path = rem[1:]
		default:
			return "", "", fmt.Errorf("invalid remote address %q: unexpected %q after port", s, rem[:1])
		}
	}

	if _, err := strconv.Atoi(port); err != nil {
		return "", "", fmt.Errorf("invalid port %q in remote address %q", port, s)
	}

	ip := net.ParseIP(host)
	if ip == nil {
		resolved, err := lookupHosts(host)
		if err != nil {
			return "", "", fmt.Errorf("cannot resolve host %q: %w", host, err)
		}
		host = resolved
	} else {
		if strings.Contains(host, ":") {
			return "", "", fmt.Errorf("IPv6 not supported: %q", host)
		}
	}

	return net.JoinHostPort(host, port), path, nil
}

func copySingle(
	ctx context.Context,
	src, dst string,
	chunkSize int64,
	batch int,
	timeout time.Duration,
	maxRetries int,
	useChecksum bool,
	recursive bool,
) error {
	srcRemote := isRemoteAddr(src)
	dstRemote := isRemoteAddr(dst)

	if !srcRemote {
		if st, stErr := os.Stat(src); stErr == nil && st.IsDir() {
			if !recursive {
				return fmt.Errorf("source is a directory; use -r to copy directories")
			}
			if !dstRemote {
				return errors.New("downloading directories is not yet supported")
			}
			hostPort, remotePath, err := parseRemoteAddr(dst)
			if err != nil {
				return err
			}
			target := remotePath
			if target == "" || strings.HasSuffix(target, "/") {
				target = target + filepath.Base(src)
			}
			logger.Log.Debug("copying directory", "host", hostPort, "src", src, "dst", target)
			return cpDirToHost(ctx, hostPort, src, target, chunkSize, batch, timeout, maxRetries, useChecksum)
		}
	}

	switch {
	case srcRemote && !dstRemote:
		hostPort, remotePath, err := parseRemoteAddr(src)
		if err != nil {
			return err
		}
		target := dst
		if target == "" || strings.HasSuffix(target, "/") {
			target = target + filepath.Base(remotePath)
		}

		isDir, dirErr := isRemoteDir(ctx, hostPort, remotePath, timeout, useChecksum)
		if dirErr == nil && isDir {
			if !recursive {
				return fmt.Errorf("source is a directory; use -r to copy directories")
			}
			return cpDirFromHost(ctx, hostPort, remotePath, target,
				chunkSize, batch, timeout, maxRetries, useChecksum)
		}

		logger.Log.Debug("downloading file", "host", hostPort, "remote", remotePath, "local", target)
		return cpOneFileFromHost(ctx, hostPort, remotePath, target,
			chunkSize, batch, timeout, maxRetries, useChecksum)

	case !srcRemote && dstRemote:
		hostPort, remotePath, err := parseRemoteAddr(dst)
		if err != nil {
			return err
		}
		target := remotePath
		if target == "" || strings.HasSuffix(target, "/") {
			target = target + filepath.Base(src)
		}
		logger.Log.Debug("uploading file", "host", hostPort, "src", src, "dst", target)
		return cpOneFileToHost(ctx, hostPort, src, target,
			chunkSize, batch, timeout, maxRetries, useChecksum)

	default:
		return errors.New("one path must be local, the other remote")
	}
}

var cpCmd = &cli.Command{
	Name:  "cp",
	Usage: "",
	Flags: []cli.Flag{
		&cli.Int64Flag{
			Name:  "chunk",
			Value: 32768,
		},
		&cli.IntFlag{
			Name:  "batch",
			Value: 4,
		},
		&cli.DurationFlag{
			Name:  "timeout",
			Value: 15 * time.Second,
			Usage: "read/write timeout (0 to disable)",
		},
		&cli.IntFlag{
			Name:  "retry",
			Value: 3,
			Usage: "max chunk transfer retries",
		},
		&cli.BoolFlag{
			Name:  "checksum",
			Value: true,
			Usage: "enable CRC-32 chunk checksums",
		},
		&cli.BoolFlag{
			Name:  "recursive, r",
			Usage: "copy directories recursively",
		},
		&cli.BoolFlag{
			Name:  "dry-run",
			Usage: "show what would be transferred without doing it",
		},
	},
	Action: func(c *cli.Context) (err error) {
		ctx := c.Context
		args := c.Args().Slice()

		if len(args) < 2 {
			return errors.New("usage: gcp cp <source>... <destination>")
		}

		sources, dst := args[:len(args)-1], args[len(args)-1]
		srcRemote := isRemoteAddr(sources[0])
		dstRemote := isRemoteAddr(dst)
		multiple := len(sources) > 1

		for _, s := range sources {
			if isRemoteAddr(s) != srcRemote {
				return fmt.Errorf("cannot mix local and remote sources")
			}
		}
		if srcRemote == dstRemote {
			return errors.New("one path must be local, the other remote")
		}

		// Expand sources into concrete file list
		var expanded []string
		if srcRemote {
			for _, s := range sources {
				hostPort, path, pErr := parseRemoteAddr(s)
				if pErr != nil {
					return pErr
				}
				matches, gErr := expandRemoteSources(ctx, hostPort, path,
					c.Duration("timeout"), c.Bool("checksum"))
				if gErr != nil {
					return gErr
				}
				expanded = append(expanded, matches...)
			}
		} else {
			for _, s := range sources {
				matches, gErr := expandLocalSource(s, c.Bool("recursive"))
				if gErr != nil {
					return gErr
				}
				expanded = append(expanded, matches...)
			}
		}

		if len(expanded) == 0 {
			return fmt.Errorf("no files matched")
		}

		if c.Bool("dry-run") {
			for _, src := range expanded {
				if srcRemote {
					fmt.Printf("dry-run: download %s to %s\n", src, dst)
				} else {
					fmt.Printf("dry-run: upload %s to %s\n", src, dst)
				}
			}
			return nil
		}

		for _, src := range expanded {
			target := dst
			if multiple {
				if srcRemote {
					_, rPath, _ := parseRemoteAddr(src)
					target = filepath.Join(dst, filepath.Base(rPath))
				} else {
					target = filepath.Join(dst, filepath.Base(src))
				}
			}
			if err := copySingle(ctx, src, target,
				c.Int64("chunk"), c.Int("batch"), c.Duration("timeout"),
				c.Int("retry"), c.Bool("checksum"), c.Bool("recursive")); err != nil {
				return err
			}
		}
		return nil
	},
}
