package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/beeleelee/gcp/asyncio"
	"github.com/beeleelee/gcp/logger"
	"github.com/panjf2000/gnet/v2"
	"github.com/urfave/cli/v2"
)

// wrappedMsg pairs a decoded protocol message with the connection it arrived
// on, so that worker goroutines can send responses back through the same conn.
type wrappedMsg struct {
	msg     asyncio.MSG
	payload []byte
	conn    gnet.Conn
}

// copierServer implements the gnet event-loop server. It receives gcp protocol
// messages from many connections and dispatches them to a pool of worker
// goroutines that perform actual file I/O.
//
// The worker pool (processNum goroutines) decouples the gnet I/O event loop
// from blocking file operations. The processMsgChan is buffered to processNum
// to prevent the event loop from blocking when all workers are busy.
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

// OnTraffic is called by gnet when data arrives on a connection. It reads
// complete gcp frames in a loop, dispatching each to the worker pool. When
// an incomplete frame is detected (ErrIncompletePacket) it returns gnet.None
// to wait for more data — gnet handles buffering internally.
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
						// unreachable — the server sends CreateRes, it never receives one.
					case asyncio.WriteReqT:
						msg, _ := (wmsg.msg).(*asyncio.WriteReq)
						c.write(wmsg.conn, msg, wmsg.payload)
					case asyncio.WriteResT:
						// unreachable — the server sends WriteRes, it never receives one.
					case asyncio.ReadReqT:
						msg, _ := (wmsg.msg).(*asyncio.ReadReq)
						c.read(wmsg.conn, msg)
					case asyncio.ReadResT:
						// unreachable — the server sends ReadRes, it never receives one.
					case asyncio.StatReqT:
						msg, _ := (wmsg.msg).(*asyncio.StatReq)
						c.stat(wmsg.conn, msg)
					case asyncio.StatResT:
						// unreachable — the server sends StatRes, it never receives one.
					case asyncio.ReadDirReqT:
						msg, _ := (wmsg.msg).(*asyncio.ReadDirReq)
						c.readDir(wmsg.conn, msg)
					case asyncio.ReadDirResT:
						// unreachable — the server sends ReadDirRes, it never receives one.
					case asyncio.HashReqT:
						msg, _ := (wmsg.msg).(*asyncio.HashReq)
						c.hash(wmsg.conn, msg)
					case asyncio.HashResT:
						// unreachable — the server sends HashRes, it never receives one.
					default:
						logger.Log.Error("should not be here, unrecognized message type", "wmsg", wmsg)
					}
				}
			}
		}(c.ctx)
	}
}

// create handles a CreateReq from the client.
//
// For directories it calls os.MkdirAll. For files it creates the parent
// directory tree, then creates or truncates the file only if it does not
// already exist (the file-already-exists path is a no-op, which supports
// upload resume where the client skips Create entirely).
func (c *copierServer) create(conn gnet.Conn, req *asyncio.CreateReq) {
	logger.Log.Debug("create called", "path", req.Path, "size", req.Size, "mode", req.Mode)

	if req.IsDir {
		if err := os.MkdirAll(req.Path, os.FileMode(req.Mode)); err != nil {
			logger.Log.Debug("failed to create directory", "err", err)
			c.createFailed(conn, req, err)
			return
		}
		c.createSuccess(conn, req)
		return
	}

	// ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(req.Path), 0755); err != nil {
		logger.Log.Debug("failed to create parent directory", "err", err)
		c.createFailed(conn, req, err)
		return
	}

	info, err := os.Stat(req.Path)
	if err == nil && info.IsDir() {
		logger.Log.Debug("create failed as target is dir", "createReq", req)
		c.createFailed(conn, req, fmt.Errorf("path is a directory"))
		return
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Log.Debug("failed to get file info", "err", err)
		c.createFailed(conn, req, err)
		return
	}

	if err != nil && errors.Is(err, os.ErrNotExist) {
		if fd, err := os.OpenFile(req.Path, os.O_CREATE|os.O_RDWR, os.FileMode(req.Mode)); err != nil {
			logger.Log.Debug("failed to open file", "err", err)
			c.createFailed(conn, req, err)
			return
		} else {
			if req.Size > 0 {
				if err := fd.Truncate(req.Size); err != nil {
					fd.Close()
					logger.Log.Debug("failed to truncate file", "err", err)
					c.createFailed(conn, req, err)
					return
				}
			}
			fd.Close()
		}
	}
	c.createSuccess(conn, req)
}

func (c *copierServer) createFailed(conn gnet.Conn, req *asyncio.CreateReq, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	if err := asyncio.WriteMessage(conn, &asyncio.CreateRes{
		ID:      req.ID,
		Success: false,
		Error:   errMsg,
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
		c.writeFailed(conn, req, fmt.Errorf("checksum mismatch"))
		return
	}
	tpath := req.Path
	fd, err := os.OpenFile(tpath, os.O_RDWR, 0644)
	if err != nil {
		logger.Log.Debug("failed to open file", "err", err)
		c.writeFailed(conn, req, err)
		return
	}
	defer fd.Close()
	n, err := fd.WriteAt(payload, req.Offset)
	if err != nil {
		logger.Log.Debug("failed to write data to file", "err", err)
		c.writeFailed(conn, req, err)
		return
	}
	c.writeSuccess(conn, req, n)
	return
}

func (c *copierServer) writeFailed(conn gnet.Conn, req *asyncio.WriteReq, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	if err := asyncio.WriteMessage(conn, &asyncio.WriteRes{
		ID:      req.ID,
		Success: false,
		Error:   errMsg,
	}, nil); err != nil {
		logger.Log.Debug("failed to write message", "err", err)
	}
}

func (c *copierServer) read(conn gnet.Conn, req *asyncio.ReadReq) {
	fd, err := os.Open(req.Path)
	if err != nil {
		logger.Log.Debug("failed to open file for read", "err", err)
		c.readFailed(conn, req, err)
		return
	}
	defer fd.Close()

	info, err := fd.Stat()
	if err != nil {
		logger.Log.Debug("failed to stat file for read", "err", err)
		c.readFailed(conn, req, err)
		return
	}

	buf := make([]byte, req.Size)
	n, err := fd.ReadAt(buf, req.Offset)
	if err != nil && n == 0 {
		logger.Log.Debug("failed to read file", "err", err)
		c.readFailed(conn, req, err)
		return
	}

	c.readSuccess(conn, req, buf[:n], info.Size())
}

func (c *copierServer) readFailed(conn gnet.Conn, req *asyncio.ReadReq, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	if err := asyncio.WriteMessage(conn, &asyncio.ReadRes{
		ID:      req.ID,
		Success: false,
		Error:   errMsg,
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
		c.readDirFailed(conn, req, err)
		return
	}
	c.readDirSuccess(conn, req, entries)
}

func (c *copierServer) readDirFailed(conn gnet.Conn, req *asyncio.ReadDirReq, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	if err := asyncio.WriteMessage(conn, &asyncio.ReadDirRes{
		ID:      req.ID,
		Success: false,
		Error:   errMsg,
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
				Name:    e.Name(),
				IsDir:   e.IsDir(),
				Mode:    uint32(info.Mode()),
				Size:    info.Size(),
				ModTime: info.ModTime().Unix(),
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
		c.statFailed(conn, req, err)
		return
	}
	c.statSuccess(conn, req, info)
}

func (c *copierServer) statFailed(conn gnet.Conn, req *asyncio.StatReq, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	if err := asyncio.WriteMessage(conn, &asyncio.StatRes{
		ID:      req.ID,
		Success: false,
		Error:   errMsg,
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

// hash handles a HashReq by computing a SHA-256 digest of the requested file
// and returning it in a HashRes. This is used for post-transfer integrity
// verification and reads the entire file into the hash state.
func (c *copierServer) hash(conn gnet.Conn, req *asyncio.HashReq) {
	fd, err := os.Open(req.Path)
	if err != nil {
		logger.Log.Debug("hash: failed to open file", "path", req.Path, "err", err)
		c.hashFailed(conn, req, err)
		return
	}
	defer fd.Close()

	h := sha256.New()
	if _, err := io.Copy(h, fd); err != nil {
		logger.Log.Debug("hash: failed to read file", "err", err)
		c.hashFailed(conn, req, err)
		return
	}

	if err := asyncio.WriteMessage(conn, &asyncio.HashRes{
		ID:      req.ID,
		Success: true,
		Hash:    h.Sum(nil),
	}, nil); err != nil {
		logger.Log.Debug("hash: failed to write response", "err", err)
	}
}

func (c *copierServer) hashFailed(conn gnet.Conn, req *asyncio.HashReq, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	if err := asyncio.WriteMessage(conn, &asyncio.HashRes{
		ID:      req.ID,
		Success: false,
		Error:   errMsg,
	}, nil); err != nil {
		logger.Log.Debug("hash: failed to write message", "err", err)
	}
}

// newServer creates a server, starts processNum worker goroutines, and
// returns the server handle. Workers are started before gnet.Run so they
// are ready to receive messages as soon as the event loop begins.
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
