package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/beeleelee/gcp/blockio"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
)

type copierServer struct {
	blockio.UnimplementedCopierServer
}

func (c *copierServer) Create(ctx context.Context, req *blockio.CreateReq) (*blockio.CreateRes, error) {
	info, err := os.Stat(req.Path)
	// failed: path exist but not a file
	if err == nil && info.IsDir() {
		return nil, errors.New(fmt.Sprintf("%s is dir", req.Path))
	}
	// failed: cannot get file info
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if err != nil && errors.Is(err, os.ErrNotExist) {
		// failed: cannot create a file by req.Path
		if fd, err := os.Create(req.Path); err != nil {
			return nil, err
		} else {
			defer fd.Close()
		}
	}

	return &blockio.CreateRes{
		Success: true,
	}, nil

}

func (c *copierServer) Write(ctx context.Context, req *blockio.WriteReq) (*blockio.WriteRes, error) {
	tpath := req.Path
	err := os.WriteFile(tpath, req.Data, 0644)
	if err != nil {
		return nil, err
	}
	return &blockio.WriteRes{
		Success: true,
		N:       int32(len(req.Data)),
	}, nil
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
