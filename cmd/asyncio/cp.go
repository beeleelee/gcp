package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/beeleelee/gcp/message"
	"github.com/beeleelee/gcp/logger"
	"github.com/urfave/cli/v2"
)

// isRemoteAddr returns true if s matches the "host:path" or "user@host:path"
// pattern used to reference remote files. The heuristic is simply the
// presence of a colon, which is sufficient since local paths on Unix
// rarely contain one.
func isRemoteAddr(s string) bool {
	return strings.Contains(s, ":")
}

// isRemoteDir checks whether a remote path is a directory by issuing a
// Stat RPC on the provided client.
func isRemoteDir(ctx context.Context, cc *copierClient, path string) (bool, error) {
	res, err := cc.Stat(path)
	if err != nil {
		return false, err
	}
	statRes := res.msg.(*message.StatRes)
	if !statRes.Success {
		return false, fmt.Errorf("stat failed for %s: %s", path, statRes.Error)
	}
	return statRes.IsDir, nil
}

// parseCompressionFlag converts a compression flag string to the asyncio
// constant. The empty string or "none" maps to CompressionNone.
func parseCompressionFlag(s string) uint8 {
	switch s {
	case "gzip":
		return message.CompressionGzip
	default:
		return message.CompressionNone
	}
}

// parseRemoteAddr splits a "user@host:/path" or "host:/path" remote address
// into hostPort, user, path, identityFile, and hostAlias. The defaultPort is
// used when --port is not specified. The host is looked up in SSH config for
// HostName resolution, User, and IdentityFile. If no SSH config match, the
// host is used as-is.
func parseRemoteAddr(s string, defaultPort string) (hostPort, userName, path, identityFile, hostAlias string, err error) {
	// Split on last @ for optional user.
	rest := s
	if atIdx := strings.LastIndex(s, "@"); atIdx >= 0 {
		userName = s[:atIdx]
		rest = s[atIdx+1:]
	}

	colonIdx := strings.Index(rest, ":")
	if colonIdx < 0 {
		return "", "", "", "", "", fmt.Errorf("invalid remote address %q: missing colon", s)
	}

	host := rest[:colonIdx]
	if host == "" {
		return "", "", "", "", "", fmt.Errorf("invalid remote address %q: empty host", s)
	}
	hostAlias = host

	path = rest[colonIdx+1:]

	// Look up host in SSH config for HostName, Port, User, IdentityFile.
	entry := sshConfigLookup(host)

	hostName := host
	port := defaultPort

	if entry != nil {
		if entry.HostName != "" {
			hostName = entry.HostName
		}
		if userName == "" && entry.User != "" {
			userName = entry.User
		}
		if entry.IdentityFile != "" {
			identityFile = entry.IdentityFile
		}
	}

	// User fallback: explicit > SSH config > current OS user.
	if userName == "" {
		current, uErr := user.Current()
		if uErr != nil {
			return "", "", "", "", "", fmt.Errorf("cannot determine current user: %w", uErr)
		}
		userName = current.Username
	}

	if net.ParseIP(hostName) == nil && strings.Contains(hostName, ":") {
		return "", "", "", "", "", fmt.Errorf("IPv6 not supported: %q", hostName)
	}

	hostPort = net.JoinHostPort(hostName, port)
	return hostPort, userName, path, identityFile, hostAlias, nil
}

// copySingle is the 4-way router that decides whether to upload, download,
// or recurse into a directory based on whether src and dst are local or
// remote filesystem paths. The shared client cc is used for all RPCs.
func copySingle(
	ctx context.Context,
	src, dst string,
	cc *copierClient,
	chunkSize int64,
	maxRetries int,
	recursive bool,
	useSha256 bool,
	compressionAlgo uint8,
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
			_, _, remotePath, _, _, err := parseRemoteAddr(dst, "5031")
			if err != nil {
				return err
			}
			target := remotePath
			if target == "" || strings.HasSuffix(target, "/") {
				target = target + filepath.Base(src)
			}
			logger.Log.Debug("copying directory", "src", src, "dst", target)
			return cpDirToHost(ctx, cc, src, target, chunkSize, maxRetries, useSha256, compressionAlgo)
		}
	}

	switch {
	case srcRemote && !dstRemote:
		_, _, remotePath, _, _, err := parseRemoteAddr(src, "5031")
		if err != nil {
			return err
		}
		target := dst
		if target == "" || strings.HasSuffix(target, "/") {
			target = target + filepath.Base(remotePath)
		}

		isDir, dirErr := isRemoteDir(ctx, cc, remotePath)
		if dirErr == nil && isDir {
			if !recursive {
				return fmt.Errorf("source is a directory; use -r to copy directories")
			}
			return cpDirFromHost(ctx, cc, remotePath, target,
				chunkSize, maxRetries, useSha256, compressionAlgo)
		}

		logger.Log.Debug("downloading file", "remote", remotePath, "local", target)
		return cpOneFileFromHost(ctx, cc, remotePath, target,
			chunkSize, maxRetries, useSha256, compressionAlgo)

	case !srcRemote && dstRemote:
		_, _, remotePath, _, _, err := parseRemoteAddr(dst, "5031")
		if err != nil {
			return err
		}
		target := remotePath
		if target == "" || strings.HasSuffix(target, "/") {
			target = target + filepath.Base(src)
		}
		logger.Log.Debug("uploading file", "src", src, "dst", target)
		return cpOneFileToHost(ctx, cc, src, target,
			chunkSize, maxRetries, useSha256, compressionAlgo)

	default:
		return errors.New("one path must be local, the other remote")
	}
}

// cpCmd implements the "cp" CLI subcommand. It parses flags, expands
// source globs (local or remote), and dispatches each source-destination
// pair to copySingle. Dry-run prints the plan without doing any I/O.
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
		&cli.BoolFlag{
			Name:  "sha256",
			Value: true,
			Usage: "verify file integrity with SHA-256 after transfer",
		},
		&cli.StringFlag{
			Name:  "compression",
			Value: "",
			Usage: "compress chunk payloads (`gzip` or empty for none)",
		},
		&cli.IntFlag{
			Name:  "port",
			Value: 5031,
			Usage: "remote port",
		},
		&cli.StringFlag{
			Name:  "identity-file",
			Usage: "SSH identity file (overrides SSH config IdentityFile)",
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
				hostPort, user, path, identityFile, hostAlias, pErr := parseRemoteAddr(s, fmt.Sprintf("%d", c.Int("port")))
				if pErr != nil {
					return pErr
				}
				_ = identityFile
				matches, gErr := expandRemoteSources(ctx, hostPort, user, hostAlias, path,
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

		// Create a shared client for all file transfers.
		port := fmt.Sprintf("%d", c.Int("port"))
		hostPort, user, _, identityFile := "", "", "", ""
		if srcRemote {
			hostPort, user, _, identityFile, _, _ = parseRemoteAddr(sources[0], port)
		} else {
			hostPort, user, _, identityFile, _, _ = parseRemoteAddr(dst, port)
		}
		if c.String("identity-file") != "" {
			identityFile = c.String("identity-file")
		}
		cc, err := newClient(ctx, hostPort, user, identityFile, c.Int("batch"), c.Duration("timeout"), c.Bool("checksum"))
		if err != nil {
			return fmt.Errorf("connect to %s: %w", hostPort, err)
		}
		defer cc.Close()

		compressionAlgo := parseCompressionFlag(c.String("compression"))
		chunkSize := c.Int64("chunk")
		maxRetries := c.Int("retry")
		useSha256 := c.Bool("sha256")
		recursive := c.Bool("recursive")

		for _, src := range expanded {
			target := dst
			if multiple {
				if srcRemote {
					_, _, _, rPath, _, _ := parseRemoteAddr(src, port)
					target = filepath.Join(dst, filepath.Base(rPath))
				} else {
					target = filepath.Join(dst, filepath.Base(src))
				}
			}
			if err := copySingle(ctx, src, target, cc,
				chunkSize, maxRetries, recursive, useSha256, compressionAlgo); err != nil {
				return err
			}
		}
		return nil
	},
}
