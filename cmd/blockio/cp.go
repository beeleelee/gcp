package main

import (
	"context"
	"fmt"

	"github.com/beeleelee/gcp/blockio"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
		&cli.StringFlag{
			Name:  "src",
			Usage: "",
		},
		&cli.StringFlag{
			Name:  "target",
			Usage: "",
		},
	},
	Action: func(c *cli.Context) (err error) {
		hostAddr := c.String("host")
		src := c.String("src")
		target := c.String("target")
		fmt.Println(hostAddr)
		fmt.Println(src, target)
		conn, err := grpc.NewClient(hostAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return
		}
		defer conn.Close()
		cc := blockio.NewCopierClient(conn)

		_, err = cc.Create(context.Background(), &blockio.CreateReq{
			Path: "./ubuntu.iso",
		})

		return err
	},
}
