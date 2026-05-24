package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/beeleelee/gcp/logger"
	"github.com/rs/zerolog"
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
				Usage: "specify minimum log level, error by default",
				Value: "error",
			},
		},
		Commands: local,
		Before: func(ctx *cli.Context) error {
			level, err := zerolog.ParseLevel(ctx.String("level"))
			if err != nil {
				return fmt.Errorf("invalid log level: %q", ctx.String("level"))
			}
			zerolog.SetGlobalLevel(level)
			return nil
		},
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := app.RunContext(ctx, os.Args); err != nil {
		log.Error("Failed to run", "error", err)
		os.Exit(1)
	}
}
