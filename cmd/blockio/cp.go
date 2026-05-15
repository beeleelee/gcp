package main

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/beeleelee/gcp/blockio"
	"github.com/beeleelee/gcp/cmd/progressbar"
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
		&cli.Int64Flag{
			Name:  "chunk",
			Value: 32768,
		},
		&cli.IntFlag{
			Name:  "batch",
			Value: 16,
		},
	},
	Action: func(c *cli.Context) (err error) {
		ctx := c.Context
		hostAddr := c.String("host")
		args := c.Args().Slice()
		src := args[0]
		target := args[1]
		fmt.Println(hostAddr)
		fmt.Println(src, target)
		if src == "" || target == "" {
			return errors.New("[usage] blockio src target")
		}
		// open the src
		sfd, err := os.Open(src)
		if err != nil {
			return
		}
		defer sfd.Close()
		// read file meta data
		sfinfo, err := sfd.Stat()
		if err != nil {
			return
		}
		conn, err := grpc.NewClient(hostAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return
		}
		defer conn.Close()
		cc := blockio.NewCopierClient(conn)
		// touch target file
		_, err = cc.Create(ctx, &blockio.CreateReq{
			Path: target,
			Mode: uint32(sfinfo.Mode()),
		})
		if err != nil {
			return
		}
		var wg sync.WaitGroup
		var chunkSize, offset int64
		chunkSize = c.Int64("chunk")
		remainSize := sfinfo.Size()
		batch := c.Int("batch")
		concurrentCtl := make(chan struct{}, batch)
		errChan := make(chan error, 0)
		progressChan := make(chan int64, batch+1)
		go progressbar.Progress(ctx, sfinfo.Size(), progressChan, time.Now(), time.Millisecond*200)
		for remainSize > 0 {
			select {
			case err = <-errChan:
				break
			default:
			}
			chus := chunkSize
			if remainSize < chus {
				chus = remainSize
			}
			wg.Add(1)
			go func(off int64, size int64) {
				defer wg.Done()
				concurrentCtl <- struct{}{}
				defer func() {
					<-concurrentCtl
				}()
				buf := make([]byte, size)
				_, err := sfd.ReadAt(buf, off)
				if err != nil {
					errChan <- err
					return
				}
				_, err = cc.Write(ctx, &blockio.WriteReq{
					Path:   target,
					Offset: off,
					Data:   buf,
				})
				if err != nil {
					errChan <- err
					return
				}
				progressChan <- size

			}(offset, chus)
			offset += chus
			remainSize -= chus
		}
		wg.Wait()

		return err
	},
}
