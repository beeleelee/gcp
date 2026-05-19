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
		return nil, fmt.Errorf("%s is dir", req.Path)
	}
	// failed: cannot get file info
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if err != nil && errors.Is(err, os.ErrNotExist) {
		fd, err := os.OpenFile(req.Path, os.O_RDWR|os.O_CREATE, os.FileMode(req.Mode))
		if err != nil {
			return nil, err
		}
		defer fd.Close()
		if req.Size > 0 {
			if err := fd.Truncate(int64(req.Size)); err != nil {
				return nil, err
			}
		}
	}

	return &blockio.CreateRes{
		Success: true,
	}, nil

}

func (c *copierServer) Write(ctx context.Context, req *blockio.WriteReq) (*blockio.WriteRes, error) {
	tpath := req.Path
	fd, err := os.OpenFile(tpath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	n, err := fd.WriteAt(req.Data, req.Offset)
	if err != nil {
		return nil, err
	}
	return &blockio.WriteRes{
		Success: true,
		N:       int32(n),
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
