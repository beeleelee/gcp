package main

import (
	"os"

	"github.com/beeleelee/gcp/logger"
	"github.com/urfave/cli/v2"
)

func main() {
	log := logger.Log
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
		log.Error("Failed to run", "error", err)
		os.Exit(1)
	}
}
