package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
		&cli.StringFlag{
			Name:  "from",
			Usage: "remote path to copy from host to local",
		},
	},
	Action: func(c *cli.Context) (err error) {
		ctx := c.Context
		hostAddr := c.String("host")
		remoteSrc := c.String("from")
		args := c.Args().Slice()

		if remoteSrc != "" {
			if len(args) < 1 {
				return errors.New("usage: blockio cp --from <remote_src> <local_target>")
			}
			src := remoteSrc
			target := args[0]
			if target == "" {
				target = filepath.Base(src)
			} else if strings.HasSuffix(target, "/") {
				target = target + filepath.Base(src)
			}
			fmt.Println(hostAddr)
			fmt.Println("remote:", src, "local:", target)

			conn, err := grpc.NewClient(hostAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()
			cc := blockio.NewCopierClient(conn)

			// first read to get file size
			res, err := cc.Read(ctx, &blockio.ReadReq{
				Path:   src,
				Offset: 0,
				Size:   c.Int64("chunk"),
			})
			if err != nil {
				return err
			}
			if !res.Success {
				return errors.New("remote file not found")
			}

			tfd, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
			if err != nil {
				return err
			}
			defer tfd.Close()

			_, err = tfd.WriteAt(res.Data, 0)
			if err != nil {
				return err
			}

			fileSize := res.FileSize
			var offset int64 = res.N
			remainSize := fileSize - offset
			chunkSize := c.Int64("chunk")
			batch := c.Int("batch")

			var wg sync.WaitGroup
			concurrentCtl := make(chan struct{}, batch)
			errChan := make(chan error, batch)
			progressChan := make(chan int64, batch+1)
			go progressbar.Progress(ctx, fileSize, progressChan, time.Now(), time.Millisecond*200)
			progressChan <- offset

		loop:
			for remainSize > 0 {
				select {
				case err = <-errChan:
					break loop
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
					res, err := cc.Read(ctx, &blockio.ReadReq{
						Path:   src,
						Offset: off,
						Size:   size,
					})
					if err != nil {
						errChan <- err
						return
					}
					if !res.Success {
						errChan <- errors.New("read failed")
						return
					}
					_, err = tfd.WriteAt(res.Data, off)
					if err != nil {
						errChan <- err
						return
					}
					progressChan <- res.N
				}(offset, chus)
				offset += chus
				remainSize -= chus
			}
			wg.Wait()

			return err
		}

		if len(args) < 2 {
			return errors.New("usage: blockio cp <src> <target>")
		}
		src := args[0]
		target := args[1]
		if target == "" {
			target = filepath.Base(src)
		} else if strings.HasSuffix(target, "/") {
			target = target + filepath.Base(src)
		}
		fmt.Println(hostAddr)
		fmt.Println(src, target)
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
			Size: uint64(sfinfo.Size()),
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
		errChan := make(chan error, batch)
		progressChan := make(chan int64, batch+1)
		go progressbar.Progress(ctx, sfinfo.Size(), progressChan, time.Now(), time.Millisecond*200)
	loop2:
		for remainSize > 0 {
			select {
			case err = <-errChan:
				break loop2
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
