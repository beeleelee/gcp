package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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
	},
	Action: func(c *cli.Context) (err error) {
		ctx := c.Context
		args := c.Args().Slice()

		if len(args) < 2 {
			return errors.New("usage: gcp cp <source> <destination>")
		}

		src, dst := args[0], args[1]

		srcRemote, dstRemote := isRemoteAddr(src), isRemoteAddr(dst)

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
			fmt.Println(hostPort)
			fmt.Println("remote:", remotePath, "local:", target)
			return cpOneFileFromHost(ctx, hostPort, remotePath, target, c.Int64("chunk"), c.Int("batch"), c.Duration("timeout"), c.Int("retry"), c.Bool("checksum"))

		case !srcRemote && dstRemote:
			hostPort, remotePath, err := parseRemoteAddr(dst)
			if err != nil {
				return err
			}
			target := remotePath
			if target == "" || strings.HasSuffix(target, "/") {
				target = target + filepath.Base(src)
			}
			fmt.Println(hostPort)
			fmt.Println(src, target)
			return cpOneFileToHost(ctx, hostPort, src, target, c.Int64("chunk"), c.Int("batch"), c.Duration("timeout"), c.Int("retry"), c.Bool("checksum"))

		default:
			return errors.New("usage: gcp cp <source> <destination>: one must be a local path, the other a remote address (host:port/path)")
		}
	},
}
