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

	"github.com/beeleelee/gcp/message"
	"github.com/beeleelee/gcp/logger"
	"github.com/panjf2000/gnet/v2"
	"github.com/urfave/cli/v2"
)

// wrappedMsg pairs a decoded protocol message with the connection it arrived
// on, so that worker goroutines can send responses back through the same conn.
type wrappedMsg struct {
	msg     message.MSG
	payload []byte
	conn    gnet.Conn
}

// authState tracks the authentication status for a single connection.
type authState int

const (
	authNew     authState = iota // waiting for initial AuthReq
	authPending                  // challenge sent, waiting for signature
	authOK                       // fully authenticated
)

// connAuth holds per-connection authentication state, stored in the gnet
// connection context.
type connAuth struct {
	state          authState
	challenge      []byte
	pubKey         []byte
	user           string
	home           string
	encryptionKey *[32]byte
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
	sessions       *sessionStore
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
	for {
		msg, payload, err := message.ReadMessage(conn)
		if err != nil {
			if err == message.ErrIncompletePacket {
				return gnet.None
			}
			return gnet.Close
		}
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

					// Auth messages bypass the auth check.
					if msgt == message.AuthReqT {
						c.handleAuth(wmsg.conn, wmsg.msg.(*message.AuthReq))
						continue
					}

					// All other messages require authentication.
					ca := getConnAuth(wmsg.conn)
					if ca == nil || ca.state != authOK {
						logger.Log.Debug("rejecting unauthenticated request", "type", msgt)
						c.rejectUnauthenticated(wmsg.conn, msgt, wmsg.msg.GetID())
						continue
					}

					switch msgt {
					case message.CreateReqT:
						msg, _ := wmsg.msg.(*message.CreateReq)
						c.create(wmsg.conn, msg, ca)
					case message.CreateResT:
						// unreachable
					case message.WriteReqT:
						msg, _ := wmsg.msg.(*message.WriteReq)
						c.write(wmsg.conn, msg, wmsg.payload, ca)
					case message.WriteResT:
						// unreachable
					case message.ReadReqT:
						msg, _ := wmsg.msg.(*message.ReadReq)
						c.read(wmsg.conn, msg, ca)
					case message.ReadResT:
						// unreachable
					case message.StatReqT:
						msg, _ := wmsg.msg.(*message.StatReq)
						c.stat(wmsg.conn, msg, ca)
					case message.StatResT:
						// unreachable
					case message.ReadDirReqT:
						msg, _ := wmsg.msg.(*message.ReadDirReq)
						c.readDir(wmsg.conn, msg, ca)
					case message.ReadDirResT:
						// unreachable
					case message.HashReqT:
						msg, _ := wmsg.msg.(*message.HashReq)
						c.hash(wmsg.conn, msg, ca)
					case message.HashResT:
						// unreachable
					default:
						logger.Log.Error("should not be here, unrecognized message type", "wmsg", wmsg)
					}
				}
			}
		}(c.ctx)
	}
}

// getConnAuth returns the connAuth stored in the gnet connection context, or
// nil if no auth state has been set yet.
func getConnAuth(conn gnet.Conn) *connAuth {
	v := conn.Context()
	if v == nil {
		return nil
	}
	ca, ok := v.(*connAuth)
	if !ok {
		return nil
	}
	return ca
}

// setConnAuth stores connAuth in the gnet connection context.
func setConnAuth(conn gnet.Conn, ca *connAuth) {
	conn.SetContext(ca)
}

// rejectUnauthenticated sends an AuthRes error for non-auth messages received
// on an unauthenticated connection, then closes the connection.
func (c *copierServer) rejectUnauthenticated(conn gnet.Conn, msgt message.MessageType, id int64) {
	errMsg := fmt.Sprintf("authentication required before type %d", msgt)
	message.WriteMessage(conn, &message.AuthRes{
		ID:      id,
		Success: false,
		Error:   errMsg,
	}, nil)
	conn.Close()
}

// handleAuth processes an AuthReq, implementing the two-phase challenge-
// response protocol and token-based fast re-auth.
func (c *copierServer) handleAuth(conn gnet.Conn, req *message.AuthReq) {
	// Phase 2 (or token auth): signature or token provided.
	if len(req.Signature) > 0 || req.Token != "" {
		// Token auth: fast path for reconnecting connections.
		if req.Token != "" {
			info := c.sessions.Get(req.Token)
			if info == nil {
				message.WriteMessage(conn, &message.AuthRes{
					ID:      req.ID,
					Success: false,
					Error:   "invalid or expired session token",
				}, nil)
				conn.Close()
				return
			}
			setConnAuth(conn, &connAuth{
				state:          authOK,
				user:           info.User,
				home:           info.Home,
				encryptionKey: info.EncryptionKey,
			})
			message.WriteMessage(conn, &message.AuthRes{
				ID:      req.ID,
				Success: true,
				User:    info.User,
			}, nil)
			return
		}

		// Signature provided — complete the challenge-response.
		ca := getConnAuth(conn)
		if ca == nil || ca.state != authPending || ca.challenge == nil {
			message.WriteMessage(conn, &message.AuthRes{
				ID:      req.ID,
				Success: false,
				Error:   "no pending challenge",
			}, nil)
			conn.Close()
			return
		}

		pubKey, err := verifySSHSignature(req.PubKey, ca.challenge, req.Signature)
		if err != nil {
			message.WriteMessage(conn, &message.AuthRes{
				ID:      req.ID,
				Success: false,
				Error:   fmt.Sprintf("signature verification failed: %v", err),
			}, nil)
			conn.Close()
			return
		}

		user, home, err := findUserByPubKey(pubKey, req.User)
		if err != nil {
			message.WriteMessage(conn, &message.AuthRes{
				ID:      req.ID,
				Success: false,
				Error:   err.Error(),
			}, nil)
			conn.Close()
			return
		}

		token, info := c.sessions.Put(user, home)
		info.EncryptionKey = deriveEncryptionKey(pubKey)
		setConnAuth(conn, &connAuth{
			state:          authOK,
			user:           info.User,
			home:           info.Home,
			encryptionKey: info.EncryptionKey,
		})

		message.WriteMessage(conn, &message.AuthRes{
			ID:      req.ID,
			Success: true,
			Token:   token,
			User:    user,
		}, nil)
		return
	}

	// Phase 1: client sent PubKey without Signature — issue a challenge.
	if len(req.PubKey) == 0 {
		message.WriteMessage(conn, &message.AuthRes{
			ID:      req.ID,
			Success: false,
			Error:   "missing public key",
		}, nil)
		conn.Close()
		return
	}

	challenge, err := generateChallenge()
	if err != nil {
		message.WriteMessage(conn, &message.AuthRes{
			ID:      req.ID,
			Success: false,
			Error:   "internal error",
		}, nil)
		conn.Close()
		return
	}

	setConnAuth(conn, &connAuth{
		state:     authPending,
		challenge: challenge,
		pubKey:    req.PubKey,
	})

	message.WriteMessage(conn, &message.AuthRes{
		ID:        req.ID,
		Success:   false,
		Challenge: challenge,
	}, nil)
}

// sandboxPath resolves a client-provided path relative to the authenticated
// user's home directory and returns the sandboxed absolute path.
func sandboxPath(ca *connAuth, reqPath string) (string, error) {
	if ca == nil {
		return "", fmt.Errorf("not authenticated")
	}
	return jailPath(ca.home, reqPath)
}

// create handles a CreateReq from the client.
func (c *copierServer) create(conn gnet.Conn, req *message.CreateReq, ca *connAuth) {
	spath, err := sandboxPath(ca, req.Path)
	if err != nil {
		c.createFailed(conn, req, err)
		return
	}

	if req.IsDir {
		if err := os.MkdirAll(spath, os.FileMode(req.Mode)); err != nil {
			logger.Log.Debug("failed to create directory", "err", err)
			c.createFailed(conn, req, err)
			return
		}
		c.createSuccess(conn, req)
		return
	}

	if err := os.MkdirAll(filepath.Dir(spath), 0755); err != nil {
		logger.Log.Debug("failed to create parent directory", "err", err)
		c.createFailed(conn, req, err)
		return
	}

	info, err := os.Stat(spath)
	if err == nil && info.IsDir() {
		c.createFailed(conn, req, fmt.Errorf("path is a directory"))
		return
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		c.createFailed(conn, req, err)
		return
	}

	if err != nil && errors.Is(err, os.ErrNotExist) {
		if fd, fdErr := os.OpenFile(spath, os.O_CREATE|os.O_RDWR, os.FileMode(req.Mode)); fdErr != nil {
			c.createFailed(conn, req, fdErr)
			return
		} else {
			if req.Size > 0 {
				if truncErr := fd.Truncate(req.Size); truncErr != nil {
					fd.Close()
					c.createFailed(conn, req, truncErr)
					return
				}
			}
			fd.Close()
		}
	}
	c.createSuccess(conn, req)
}

func (c *copierServer) createFailed(conn gnet.Conn, req *message.CreateReq, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	message.WriteMessage(conn, &message.CreateRes{
		ID:      req.ID,
		Success: false,
		Error:   errMsg,
	}, nil)
}

func (c *copierServer) createSuccess(conn gnet.Conn, req *message.CreateReq) {
	message.WriteMessage(conn, &message.CreateRes{
		ID:      req.ID,
		Success: true,
	}, nil)
}

func (c *copierServer) write(conn gnet.Conn, req *message.WriteReq, payload []byte, ca *connAuth) {
	if req.Checksum != 0 && crc32.ChecksumIEEE(payload) != req.Checksum {
		c.writeFailed(conn, req, fmt.Errorf("checksum mismatch"))
		return
	}

	if req.Encryption != message.EncryptionSecretBox {
		c.writeFailed(conn, req, fmt.Errorf("unencrypted payload rejected"))
		return
	}

	decrypted, err := decryptChunk(payload, ca.encryptionKey)
	if err != nil {
		c.writeFailed(conn, req, fmt.Errorf("decrypt: %w", err))
		return
	}

	data, err := decompressChunk(decrypted, req.Compression)
	if err != nil {
		c.writeFailed(conn, req, err)
		return
	}

	spath, err := sandboxPath(ca, req.Path)
	if err != nil {
		c.writeFailed(conn, req, err)
		return
	}

	fd, err := os.OpenFile(spath, os.O_RDWR, 0644)
	if err != nil {
		c.writeFailed(conn, req, err)
		return
	}
	defer fd.Close()
	n, err := fd.WriteAt(data, req.Offset)
	if err != nil {
		c.writeFailed(conn, req, err)
		return
	}
	c.writeSuccess(conn, req, n)
}

func (c *copierServer) writeFailed(conn gnet.Conn, req *message.WriteReq, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	message.WriteMessage(conn, &message.WriteRes{
		ID:      req.ID,
		Success: false,
		Error:   errMsg,
	}, nil)
}

func (c *copierServer) read(conn gnet.Conn, req *message.ReadReq, ca *connAuth) {
	if req.Encryption != message.EncryptionSecretBox {
		c.readFailed(conn, req, fmt.Errorf("unencrypted request rejected"))
		return
	}

	spath, err := sandboxPath(ca, req.Path)
	if err != nil {
		c.readFailed(conn, req, err)
		return
	}

	fd, err := os.Open(spath)
	if err != nil {
		c.readFailed(conn, req, err)
		return
	}
	defer fd.Close()

	info, err := fd.Stat()
	if err != nil {
		c.readFailed(conn, req, err)
		return
	}

	buf := make([]byte, req.Size)
	n, err := fd.ReadAt(buf, req.Offset)
	if err != nil && n == 0 {
		c.readFailed(conn, req, err)
		return
	}

	data := buf[:n]
	compressed, algo, err := compressChunk(data, req.Compression)
	if err != nil {
		c.readFailed(conn, req, err)
		return
	}

	encrypted, err := encryptChunk(compressed, ca.encryptionKey)
	if err != nil {
		c.readFailed(conn, req, fmt.Errorf("encrypt: %w", err))
		return
	}

	c.readSuccess(conn, req, encrypted, algo, message.EncryptionSecretBox, info.Size())
}

func (c *copierServer) readFailed(conn gnet.Conn, req *message.ReadReq, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	message.WriteMessage(conn, &message.ReadRes{
		ID:      req.ID,
		Success: false,
		Error:   errMsg,
	}, nil)
}

func (c *copierServer) readSuccess(conn gnet.Conn, req *message.ReadReq, data []byte, compressionAlgo, encryptionAlgo uint8, fileSize int64) {
	checksum := crc32.ChecksumIEEE(data)
	message.WriteMessage(conn, &message.ReadRes{
		ID:          req.ID,
		Success:     true,
		N:           int64(len(data)),
		Checksum:    checksum,
		Compression: compressionAlgo,
		Encryption:  encryptionAlgo,
	}, data)
}

func (c *copierServer) readDir(conn gnet.Conn, req *message.ReadDirReq, ca *connAuth) {
	spath, err := sandboxPath(ca, req.Path)
	if err != nil {
		c.readDirFailed(conn, req, err)
		return
	}

	entries, err := os.ReadDir(spath)
	if err != nil {
		c.readDirFailed(conn, req, err)
		return
	}
	c.readDirSuccess(conn, req, entries)
}

func (c *copierServer) readDirFailed(conn gnet.Conn, req *message.ReadDirReq, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	message.WriteMessage(conn, &message.ReadDirRes{
		ID:      req.ID,
		Success: false,
		Error:   errMsg,
	}, nil)
}

func (c *copierServer) readDirSuccess(conn gnet.Conn, req *message.ReadDirReq, entries []os.DirEntry) {
	res := &message.ReadDirRes{
		ID:      req.ID,
		Success: true,
		Entries: make([]message.DirEntry, len(entries)),
	}
	for i, e := range entries {
		info, infoErr := e.Info()
		if infoErr != nil {
			res.Entries[i] = message.DirEntry{
				Name:  e.Name(),
				IsDir: e.IsDir(),
			}
		} else {
			res.Entries[i] = message.DirEntry{
				Name:    e.Name(),
				IsDir:   e.IsDir(),
				Mode:    uint32(info.Mode()),
				Size:    info.Size(),
				ModTime: info.ModTime().Unix(),
			}
		}
	}
	message.WriteMessage(conn, res, nil)
}

func (c *copierServer) stat(conn gnet.Conn, req *message.StatReq, ca *connAuth) {
	spath, err := sandboxPath(ca, req.Path)
	if err != nil {
		c.statFailed(conn, req, err)
		return
	}

	info, err := os.Stat(spath)
	if err != nil {
		c.statFailed(conn, req, err)
		return
	}
	c.statSuccess(conn, req, info)
}

func (c *copierServer) statFailed(conn gnet.Conn, req *message.StatReq, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	message.WriteMessage(conn, &message.StatRes{
		ID:      req.ID,
		Success: false,
		Error:   errMsg,
	}, nil)
}

func (c *copierServer) statSuccess(conn gnet.Conn, req *message.StatReq, info os.FileInfo) {
	message.WriteMessage(conn, &message.StatRes{
		ID:      req.ID,
		Success: true,
		Size:    info.Size(),
		Mode:    uint32(info.Mode()),
		IsDir:   info.IsDir(),
	}, nil)
}

func (c *copierServer) writeSuccess(conn gnet.Conn, req *message.WriteReq, n int) {
	message.WriteMessage(conn, &message.WriteRes{
		ID:      req.ID,
		Success: true,
		N:       int32(n),
	}, nil)
}

func (c *copierServer) hash(conn gnet.Conn, req *message.HashReq, ca *connAuth) {
	spath, err := sandboxPath(ca, req.Path)
	if err != nil {
		c.hashFailed(conn, req, err)
		return
	}

	fd, err := os.Open(spath)
	if err != nil {
		c.hashFailed(conn, req, err)
		return
	}
	defer fd.Close()

	h := sha256.New()
	if _, err := io.Copy(h, fd); err != nil {
		c.hashFailed(conn, req, err)
		return
	}

	message.WriteMessage(conn, &message.HashRes{
		ID:      req.ID,
		Success: true,
		Hash:    h.Sum(nil),
	}, nil)
}

func (c *copierServer) hashFailed(conn gnet.Conn, req *message.HashReq, err error) {
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	message.WriteMessage(conn, &message.HashRes{
		ID:      req.ID,
		Success: false,
		Error:   errMsg,
	}, nil)
}

func newServer(ctx context.Context, processNum int) *copierServer {
	cs := &copierServer{
		processMsgChan: make(chan *wrappedMsg, processNum),
		ctx:            ctx,
		processNum:     processNum,
		sessions:       newSessionStore(),
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
			Value: "tcp://0.0.0.0:5031",
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
