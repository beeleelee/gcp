package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v2"
)

var cpCmd = &cli.Command{
	Name:  "cp",
	Usage: "",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "host",
			Usage: "",
			Value: "localhost:1717",
		},
		&cli.Int64Flag{
			Name:  "chunk",
			Value: 32768,
		},
		&cli.IntFlag{
			Name:  "batch",
			Value: 4,
		},
	},
	Action: func(c *cli.Context) (err error) {
		ctx := c.Context
		hostAddr := c.String("host")
		args := c.Args().Slice()
		if len(args) < 2 {
			return errors.New("usage: gcp cp <src> <target>")
		}
		src := args[0]
		target := args[1]
		if target == "" {
			target = filepath.Base(src)
		} else if strings.HasSuffix(target, "/") {
			target = target + filepath.Base(src)
		}
		fmt.Println(hostAddr)
		fmt.Println(src, target)
		return cpOneFileToHost(ctx, hostAddr, src, target, c.Int64("chunk"), c.Int("batch"))
	},
}
