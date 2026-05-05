package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/beeleelee/gcp/blockio"
	"github.com/panjf2000/gnet/v2"
	"github.com/urfave/cli/v2"
)

type copierServer struct {
	gnet.BuiltinEventEngine

	eng gnet.Engine
}

func (c *copierServer) OnBoot(eng gnet.Engine) gnet.Action {
	c.eng = eng
	return gnet.None
}

func (c *copierServer) OnTraffic(conn gnet.Conn) gnet.Action {
	return gnet.None
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
		if fd, err := os.OpenFile(req.Path, os.O_CREATE|os.O_RDONLY, os.FileMode(req.Mode)); err != nil {
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
		multicore := true
		return gnet.Run(newServer(), listenAddr, gnet.WithMulticore(multicore))
	},
}
