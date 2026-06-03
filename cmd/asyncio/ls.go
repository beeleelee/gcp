package main

import (
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/beeleelee/gcp/asyncio"
	"github.com/dustin/go-humanize"
	"github.com/urfave/cli/v2"
)

func modeString(mode uint32) string {
	return fs.FileMode(mode).String()
}

func sizeString(size int64, human bool) string {
	if human {
		return fmt.Sprintf("%6s", humanize.IBytes(uint64(size)))
	}
	return fmt.Sprintf("%6d", size)
}

func timeString(unix int64) string {
	if unix == 0 {
		return strings.Repeat(" ", 16)
	}
	return time.Unix(unix, 0).Format("2006-01-02 15:04")
}

func printEntry(entry asyncio.DirEntry, long, human bool) {
	name := entry.Name
	if entry.IsDir {
		name += "/"
	}
	if long {
		fmt.Printf("%s  %s  %s  %s\n",
			modeString(entry.Mode),
			sizeString(entry.Size, human),
			timeString(entry.ModTime),
			name)
	} else {
		fmt.Println(name)
	}
}

var lsCmd = &cli.Command{
	Name:  "ls",
	Usage: "list remote path contents",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "long, l",
			Usage: "show detailed listing",
		},
		&cli.BoolFlag{
			Name:  "human-readable, h",
			Usage: "print human-readable sizes (with -l)",
		},
	},
	Action: func(c *cli.Context) error {
		if c.Args().Len() < 1 {
			return fmt.Errorf("usage: gcp ls [--long] <remote_path>")
		}

		hostPort, path, err := parseRemoteAddr(c.Args().First())
		if err != nil {
			return err
		}

		long := c.Bool("long")
		human := c.Bool("human-readable")

		cc, err := newClient(c.Context, hostPort, 1, 0, false)
		if err != nil {
			return err
		}
		defer cc.Close()

		res, err := cc.Stat(path)
		if err != nil {
			return err
		}
		statRes := res.msg.(*asyncio.StatRes)
		if !statRes.Success {
			return fmt.Errorf("stat failed for %s: %s", path, statRes.Error)
		}

		if statRes.IsDir {
			res, err := cc.ReadDir(path)
			if err != nil {
				return err
			}
			dirRes := res.msg.(*asyncio.ReadDirRes)
			if !dirRes.Success {
				return fmt.Errorf("readdir failed for %s: %s", path, dirRes.Error)
			}

			sort.Slice(dirRes.Entries, func(i, j int) bool {
				return dirRes.Entries[i].Name < dirRes.Entries[j].Name
			})

			for _, entry := range dirRes.Entries {
				printEntry(entry, long, human)
			}
		} else {
			printEntry(asyncio.DirEntry{
				Name:  path,
				IsDir: false,
				Mode:  statRes.Mode,
				Size:  statRes.Size,
			}, long, human)
		}

		return nil
	},
}
