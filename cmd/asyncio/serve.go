package main

import (
	"context"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"

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
	logger.Log.Debug("OnTraffic called")
	for {
		msg, payload, err := asyncio.ReadMessage(conn)
		if err != nil {
			if err == asyncio.ErrIncompletePacket {
				logger.Log.Debug("OnTraffic incomplete packet, returning")
				return gnet.None
			}
			logger.Log.Debug("OnTraffic error", "err", err)
			return gnet.Close
		}
		logger.Log.Debug("OnTraffic got msg", "type", msg.Type())
		c.processMsgChan <- &wrappedMsg{
			msg:     msg,
			payload: payload,
			conn:    conn,
		}
	}
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
					case asyncio.ReadReqT:
						msg, _ := (wmsg.msg).(*asyncio.ReadReq)
						c.read(wmsg.conn, msg)
					case asyncio.ReadResT:
						// for now, do nothing
					case asyncio.StatReqT:
						msg, _ := (wmsg.msg).(*asyncio.StatReq)
						c.stat(wmsg.conn, msg)
					case asyncio.StatResT:
						// for now, do nothing
					case asyncio.ReadDirReqT:
						msg, _ := (wmsg.msg).(*asyncio.ReadDirReq)
						c.readDir(wmsg.conn, msg)
					case asyncio.ReadDirResT:
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
	logger.Log.Debug("create called", "path", req.Path, "size", req.Size, "mode", req.Mode)

	if req.IsDir {
		if err := os.MkdirAll(req.Path, os.FileMode(req.Mode)); err != nil {
			logger.Log.Debug("failed to create directory", "err", err)
			c.createFailed(conn, req)
			return
		}
		c.createSuccess(conn, req)
		return
	}

	// ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(req.Path), 0755); err != nil {
		logger.Log.Debug("failed to create parent directory", "err", err)
		c.createFailed(conn, req)
		return
	}

	info, err := os.Stat(req.Path)
	if err == nil && info.IsDir() {
		logger.Log.Debug("create failed as target is dir", "createReq", req)
		c.createFailed(conn, req)
		return
	}
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
	logger.Log.Debug("createSuccess called, sending response", "id", req.ID)
	if err := asyncio.WriteMessage(conn, &asyncio.CreateRes{
		ID:      req.ID,
		Success: true,
	}, nil); err != nil {
		logger.Log.Debug("failed to write message", "err", err)
	} else {
		logger.Log.Debug("WriteMessage returned successfully for CreateRes")
	}
}

func (c *copierServer) write(conn gnet.Conn, req *asyncio.WriteReq, payload []byte) {
	if req.Checksum != 0 && crc32.ChecksumIEEE(payload) != req.Checksum {
		logger.Log.Debug("checksum mismatch, rejecting write", "path", req.Path, "offset", req.Offset)
		c.writeFailed(conn, req)
		return
	}
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

func (c *copierServer) read(conn gnet.Conn, req *asyncio.ReadReq) {
	fd, err := os.Open(req.Path)
	if err != nil {
		logger.Log.Debug("failed to open file for read", "err", err)
		c.readFailed(conn, req)
		return
	}
	defer fd.Close()

	info, err := fd.Stat()
	if err != nil {
		logger.Log.Debug("failed to stat file for read", "err", err)
		c.readFailed(conn, req)
		return
	}

	buf := make([]byte, req.Size)
	n, err := fd.ReadAt(buf, req.Offset)
	if err != nil && n == 0 {
		logger.Log.Debug("failed to read file", "err", err)
		c.readFailed(conn, req)
		return
	}

	c.readSuccess(conn, req, buf[:n], info.Size())
}

func (c *copierServer) readFailed(conn gnet.Conn, req *asyncio.ReadReq) {
	if err := asyncio.WriteMessage(conn, &asyncio.ReadRes{
		ID:      req.ID,
		Success: false,
	}, nil); err != nil {
		logger.Log.Debug("failed to write message", "err", err)
	}
}

func (c *copierServer) readSuccess(conn gnet.Conn, req *asyncio.ReadReq, data []byte, fileSize int64) {
	checksum := crc32.ChecksumIEEE(data)

	if err := asyncio.WriteMessage(conn, &asyncio.ReadRes{
		ID:       req.ID,
		Success:  true,
		N:        int64(len(data)),
		Checksum: checksum,
	}, data); err != nil {
		logger.Log.Debug("failed to write message", "err", err)
	}
}

func (c *copierServer) readDir(conn gnet.Conn, req *asyncio.ReadDirReq) {
	entries, err := os.ReadDir(req.Path)
	if err != nil {
		logger.Log.Debug("readdir failed", "path", req.Path, "err", err)
		c.readDirFailed(conn, req)
		return
	}
	c.readDirSuccess(conn, req, entries)
}

func (c *copierServer) readDirFailed(conn gnet.Conn, req *asyncio.ReadDirReq) {
	if err := asyncio.WriteMessage(conn, &asyncio.ReadDirRes{
		ID:      req.ID,
		Success: false,
	}, nil); err != nil {
		logger.Log.Debug("failed to write message", "err", err)
	}
}

func (c *copierServer) readDirSuccess(conn gnet.Conn, req *asyncio.ReadDirReq, entries []os.DirEntry) {
	res := &asyncio.ReadDirRes{
		ID:      req.ID,
		Success: true,
		Entries: make([]asyncio.DirEntry, len(entries)),
	}
	for i, e := range entries {
		info, infoErr := e.Info()
		if infoErr != nil {
			res.Entries[i] = asyncio.DirEntry{
				Name:  e.Name(),
				IsDir: e.IsDir(),
			}
		} else {
			res.Entries[i] = asyncio.DirEntry{
				Name:  e.Name(),
				IsDir: e.IsDir(),
				Mode:  uint32(info.Mode()),
			}
		}
	}
	if err := asyncio.WriteMessage(conn, res, nil); err != nil {
		logger.Log.Debug("failed to write message", "err", err)
	}
}

func (c *copierServer) stat(conn gnet.Conn, req *asyncio.StatReq) {
	info, err := os.Stat(req.Path)
	if err != nil {
		logger.Log.Debug("stat failed", "path", req.Path, "err", err)
		c.statFailed(conn, req)
		return
	}
	c.statSuccess(conn, req, info)
}

func (c *copierServer) statFailed(conn gnet.Conn, req *asyncio.StatReq) {
	if err := asyncio.WriteMessage(conn, &asyncio.StatRes{
		ID:      req.ID,
		Success: false,
	}, nil); err != nil {
		logger.Log.Debug("failed to write message", "err", err)
	}
}

func (c *copierServer) statSuccess(conn gnet.Conn, req *asyncio.StatReq, info os.FileInfo) {
	if err := asyncio.WriteMessage(conn, &asyncio.StatRes{
		ID:      req.ID,
		Success: true,
		Size:    info.Size(),
		Mode:    uint32(info.Mode()),
		IsDir:   info.IsDir(),
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

		srv := newServer(c.Context, processNum)

		go func() {
			<-c.Context.Done()
			logger.Log.Info("shutting down gracefully...")
			srv.eng.Stop(c.Context)
		}()

		err = gnet.Run(srv, listenAddr, gnet.WithMulticore(multicore))
		if err == nil {
			logger.Log.Info("", "listen", listenAddr, "process", processNum, "multicore", multicore)
		}
		return
	},
}
