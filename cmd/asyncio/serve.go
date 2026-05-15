package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/beeleelee/gcp/asyncio"
	"github.com/panjf2000/gnet/v2"
	"github.com/urfave/cli/v2"
)

type wrappedMsg struct {
	msg     asyncio.MSG
	payload []byte
	conn    gnet.Conn
}

type copierServer struct {
	gnet.BuiltinEventEngine

	eng            gnet.Engine
	ctx            context.Context
	processMsgChan chan *wrappedMsg
	processNum     int
}

func (c *copierServer) OnBoot(eng gnet.Engine) gnet.Action {
	c.eng = eng
	return gnet.None
}

func (c *copierServer) OnTraffic(conn gnet.Conn) gnet.Action {
	msg, payload, err := asyncio.ReadMessage(conn)
	if err != nil {
		if err == asyncio.ErrIncompletePacket {
			return gnet.None
		}
		return gnet.Close
	}
	c.processMsgChan <- &wrappedMsg{
		msg:     msg,
		payload: payload,
		conn:    conn,
	}
	return gnet.None
}

func (c *copierServer) process() {
	for i := 0; i < c.processNum; i++ {
		fmt.Printf("set process %d\n", i)
		go func(ctx context.Context) {

			for {
				select {
				case <-ctx.Done():
					return
				case wmsg := <-c.processMsgChan:
					msgt := wmsg.msg.Type()
					switch msgt {
					case asyncio.CreateReqT:
						msg, _ := (wmsg.msg).(*asyncio.CreateReq)
						c.create(wmsg.conn, msg)
					case asyncio.CreateResT:
						// for now, do nothing
					case asyncio.WriteReqT:
						msg, _ := (wmsg.msg).(*asyncio.WriteReq)
						c.write(wmsg.conn, msg, wmsg.payload)
					case asyncio.WriteResT:
						// for now, do nothing
					default:
						fmt.Println("should not be here, unrecognized message type")
					}
				}
			}
		}(c.ctx)
	}
}

func (c *copierServer) create(conn gnet.Conn, req *asyncio.CreateReq) {
	info, err := os.Stat(req.Path)
	// failed: path exist but not a file
	if err == nil && info.IsDir() {
		fmt.Printf("%s is dir\n", req.Path)
		c.createFailed(conn, req)
		return
	}
	// failed: cannot get file info
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		fmt.Println(err)
		c.createFailed(conn, req)
		return
	}

	if err != nil && errors.Is(err, os.ErrNotExist) {
		// failed: cannot create a file by req.Path
		if fd, err := os.OpenFile(req.Path, os.O_CREATE|os.O_RDONLY, os.FileMode(req.Mode)); err != nil {
			fmt.Println(err)
			c.createFailed(conn, req)
			return
		} else {
			if req.Size > 0 {
				fd.Truncate(req.Size)
			}
			defer fd.Close()
		}
	}

	c.createSuccess(conn, req)

}

func (c *copierServer) createFailed(conn gnet.Conn, req *asyncio.CreateReq) {
	if err := asyncio.WriteMessage(conn, &asyncio.CreateRes{
		ID:      req.ID,
		Success: false,
	}, nil); err != nil {
		fmt.Println(err)
	}
}

func (c *copierServer) createSuccess(conn gnet.Conn, req *asyncio.CreateReq) {
	if err := asyncio.WriteMessage(conn, &asyncio.CreateRes{
		ID:      req.ID,
		Success: true,
	}, nil); err != nil {
		fmt.Println(err)
	}
}

func (c *copierServer) write(conn gnet.Conn, req *asyncio.WriteReq, payload []byte) {
	tpath := req.Path
	fd, err := os.OpenFile(tpath, os.O_RDWR, 0644)
	if err != nil {
		fmt.Println(err)
		c.writeFailed(conn, req)
		return
	}
	defer fd.Close()
	n, err := fd.WriteAt(payload, req.Offset)
	if err != nil {
		fmt.Println(err)
		c.writeFailed(conn, req)
		return
	}
	c.writeSuccess(conn, req, n)
	return
}

func (c *copierServer) writeFailed(conn gnet.Conn, req *asyncio.WriteReq) {
	if err := asyncio.WriteMessage(conn, &asyncio.WriteRes{
		ID:      req.ID,
		Success: false,
	}, nil); err != nil {
		fmt.Println(err)
	}
}

func (c *copierServer) writeSuccess(conn gnet.Conn, req *asyncio.WriteReq, n int) {
	if err := asyncio.WriteMessage(conn, &asyncio.WriteRes{
		ID:      req.ID,
		Success: true,
		N:       int32(n),
	}, nil); err != nil {
		fmt.Println(err)
	}
}

func newServer(ctx context.Context, processNum int) *copierServer {
	cs := &copierServer{
		processMsgChan: make(chan *wrappedMsg, processNum),
		ctx:            ctx,
		processNum:     processNum,
	}
	cs.process()
	return cs
}

var serveCmd = &cli.Command{
	Name:  "serve",
	Usage: "",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "listen",
			Usage: "",
			Value: "tcp://0.0.0.0:1717",
		},
	},
	Action: func(c *cli.Context) (err error) {
		listenAddr := c.String("listen")
		fmt.Println("listen to ", listenAddr)
		multicore := true
		return gnet.Run(newServer(c.Context, 4), listenAddr, gnet.WithMulticore(multicore))
	},
}
