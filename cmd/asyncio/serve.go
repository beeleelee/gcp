package main

import (
	"context"
	"errors"
	"os"

	"github.com/beeleelee/gcp/asyncio"
	"github.com/beeleelee/gcp/logger"
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
						logger.Log.Error("should not be here, unrecognized message type", "wmsg", wmsg)
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
		logger.Log.Debug("create failed as target is dir", "createReq", req)
		c.createFailed(conn, req)
		return
	}
	// failed: cannot get file info
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Log.Debug("failed to get file info", "err", err)
		c.createFailed(conn, req)
		return
	}

	if err != nil && errors.Is(err, os.ErrNotExist) {
		if fd, err := os.OpenFile(req.Path, os.O_CREATE|os.O_RDWR, os.FileMode(req.Mode)); err != nil {
			logger.Log.Debug("failed to open file", "err", err)
			c.createFailed(conn, req)
			return
		} else {
			if req.Size > 0 {
				if err := fd.Truncate(req.Size); err != nil {
					fd.Close()
					logger.Log.Debug("failed to truncate file", "err", err)
					c.createFailed(conn, req)
					return
				}
			}
			fd.Close()
		}
	}
	c.createSuccess(conn, req)
}

func (c *copierServer) createFailed(conn gnet.Conn, req *asyncio.CreateReq) {
	if err := asyncio.WriteMessage(conn, &asyncio.CreateRes{
		ID:      req.ID,
		Success: false,
	}, nil); err != nil {
		logger.Log.Debug("failed to write message", "err", err)
	}
}

func (c *copierServer) createSuccess(conn gnet.Conn, req *asyncio.CreateReq) {
	if err := asyncio.WriteMessage(conn, &asyncio.CreateRes{
		ID:      req.ID,
		Success: true,
	}, nil); err != nil {
		logger.Log.Debug("failed to write message", "err", err)
	}
}

func (c *copierServer) write(conn gnet.Conn, req *asyncio.WriteReq, payload []byte) {
	tpath := req.Path
	fd, err := os.OpenFile(tpath, os.O_RDWR, 0644)
	if err != nil {
		logger.Log.Debug("failed to open file", "err", err)
		c.writeFailed(conn, req)
		return
	}
	defer fd.Close()
	n, err := fd.WriteAt(payload, req.Offset)
	if err != nil {
		logger.Log.Debug("failed to write data to file", "err", err)
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
		logger.Log.Debug("failed to write message", "err", err)
	}
}

func (c *copierServer) writeSuccess(conn gnet.Conn, req *asyncio.WriteReq, n int) {
	if err := asyncio.WriteMessage(conn, &asyncio.WriteRes{
		ID:      req.ID,
		Success: true,
		N:       int32(n),
	}, nil); err != nil {
		logger.Log.Debug("failed to write message", "err", err)
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
		&cli.IntFlag{
			Name:  "process-num",
			Value: 4,
		},
		&cli.BoolFlag{
			Name:  "multicore",
			Value: true,
		},
	},
	Action: func(c *cli.Context) (err error) {
		listenAddr := c.String("listen")
		processNum := c.Int("process-num")
		multicore := c.Bool("multicore")
		err = gnet.Run(newServer(c.Context, processNum), listenAddr, gnet.WithMulticore(multicore))
		if err == nil {
			logger.Log.Info("", "listen", listenAddr, "process", processNum, "multicore", multicore)
		}
		return
	},
}
