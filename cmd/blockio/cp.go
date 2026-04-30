package main

import (
	"context"
	"errors"
	"fmt"
	"os"

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
	},
	Action: func(c *cli.Context) (err error) {
		hostAddr := c.String("host")
		args := c.Args().Slice()
		src := args[0]
		target := args[1]
		fmt.Println(hostAddr)
		fmt.Println(src, target)
		if src == "" || target == "" {
			return errors.New("[usage] blockio src target")
		}
		// // open the src
		// sfd, err := os.Open(src)
		// if err != nil {
		// 	return
		// }
		// defer sfd.Close()
		conn, err := grpc.NewClient(hostAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return
		}
		defer conn.Close()
		cc := blockio.NewCopierClient(conn)

		_, err = cc.Create(context.Background(), &blockio.CreateReq{
			Path: target,
		})
		if err != nil {
			return
		}
		data, err := os.ReadFile(src)
		if err != nil {
			return
		}
		_, err = cc.Write(context.Background(), &blockio.WriteReq{
			Path:   target,
			Offset: 0,
			Data:   data,
		})
		if err != nil {
			return
		}
		return nil
	},
}
