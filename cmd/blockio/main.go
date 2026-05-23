package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	fmt.Fprintln(os.Stderr, "WARNING: blockio is deprecated. Use 'gcp' (asyncio) instead.")
	local := []*cli.Command{
		serveCmd,
		cpCmd,
	}
	app := &cli.App{
		Name: "gcp",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "level, L",
				Usage: "specify minimum log level, INFO by default",
				Value: "error",
			},
		},
		Commands: local,
		Before: func(ctx *cli.Context) error {
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
}
