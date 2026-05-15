package main

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/urfave/cli/v2"
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
			Value: 2,
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
			return errors.New("[usage] asyincio src target")
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
		batch := c.Int("batch")
		cc := newClient(ctx, target, batch)

		// touch target file
		_, err = cc.Create(target, sfinfo.Size(), sfinfo.Mode())
		if err != nil {
			return
		}
		var wg sync.WaitGroup
		var chunkSize, offset int64
		chunkSize = c.Int64("chunk")
		remainSize := sfinfo.Size()

		concurrentCtl := make(chan struct{}, batch)
		errChan := make(chan error, 0)
		progressChan := make(chan int64, batch+1)
		startTime := time.Now()

		// print progress
		go func() {
			total := sfinfo.Size()
			var sent int64
			ticker := time.Tick(time.Millisecond * 200)
			var timeSpan float64
			for {
				select {
				case <-ctx.Done():
					return
				case cur := <-ticker:
					timeSpan = cur.Sub(startTime).Seconds()
				case size := <-progressChan:
					sent += size
					fmt.Printf("\rTotal: %s Sent: %s  Completed: %.0f%% elapsed: %.2fs", humanize.IBytes(uint64(total)), humanize.IBytes(uint64(sent)), float64(sent*100)/float64(total), timeSpan)
					if sent == total {
						return
					}
				}
			}
		}()

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
				_, err = cc.Write(target, off, buf)
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
