package main

import (
	"context"
	"fmt"
	"net"

	"github.com/beeleelee/gcp/blockio"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
)

type copierServer struct {
	blockio.UnimplementedCopierServer
}

func (c *copierServer) Create(ctx context.Context, req *blockio.CreateReq) (*blockio.CreateRes, error) {
	fmt.Println("create request received")
	return nil, nil
}

func newServer() *copierServer {
	return &copierServer{}
}

var serveCmd = &cli.Command{
	Name:  "serve",
	Usage: "",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "listen",
			Usage: "",
			Value: ":1717",
		},
	},
	Action: func(c *cli.Context) (err error) {
		listenAddr := c.String("listen")
		fmt.Println("listen to ", listenAddr)
		l, err := net.Listen("tcp", listenAddr)
		if err != nil {
			return
		}
		grpcServer := grpc.NewServer()
		blockio.RegisterCopierServer(grpcServer, newServer())
		grpcServer.Serve(l)
		return nil
	},
}
