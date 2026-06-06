package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beeleelee/gcp/message"
	"github.com/beeleelee/gcp/logger"
	"golang.org/x/crypto/ssh"
)

// randReader is used for SSH signing operations.
var randReader = rand.Reader

// clientWrappedMsg pairs a decoded protocol message with its payload.
type clientWrappedMsg struct {
	msg     message.MSG
	payload []byte
}

// clientRequestMsg wraps a clientWrappedMsg with a response channel so that
// processMsg can route the server's response back to the waiting caller.
type clientRequestMsg struct {
	clientWrappedMsg
	resChan chan clientWrappedMsg
}

// copierClient is a message-oriented RPC client built on a pool of TCP
// connections. It uses three channels to decouple callers, message routing,
// and network I/O:
//
//	msgIn         — entry point for outgoing RPC requests (produced by callers)
//	sendHandle    — messages ready to be written to a TCP connection
//	receiveHandle — incoming responses from TCP connections, ready for routing
//
// The processMsg goroutine sits between msgIn and (sendHandle, receiveHandle),
// matching request IDs to response channels.
//
// batch controls the number of concurrent TCP connections and thus the
// maximum parallelism for chunk transfers.
type copierClient struct {
	ctx           context.Context
	cancel        context.CancelFunc
	target        string
	id            int64
	batch         int
	timeout       time.Duration
	useChecksum   bool
	useEncryption bool
	authToken     string
	authUser      string
	identityFile  string
	encryptionKey *[32]byte
	msgIn         chan clientRequestMsg
	sendHandle    chan clientWrappedMsg
	receiveHandle chan clientWrappedMsg
}

func newClient(ctx context.Context, target, user, identityFile string, batch int, timeout time.Duration, useChecksum bool, useEncryption bool) (*copierClient, error) {
	cc := &copierClient{
		ctx:           ctx,
		target:        target,
		batch:         batch,
		timeout:       timeout,
		useChecksum:   useChecksum,
		useEncryption: useEncryption,
		authUser:      user,
		identityFile:  identityFile,
		msgIn:         make(chan clientRequestMsg),
		sendHandle:    make(chan clientWrappedMsg),
		receiveHandle: make(chan clientWrappedMsg),
	}
	cc.ctx, cc.cancel = context.WithCancel(ctx)
	if err := cc.dialAndAuth(); err != nil {
		cc.cancel()
		return nil, err
	}
	go cc.processMsg()
	return cc, nil
}

// processMsg is the message routing hub. It maintains a resCache mapping
// message IDs to response channels. Outgoing requests are forwarded to
// sendHandle (and their channel stored), incoming responses are matched by
// ID and delivered to the waiting caller via the cached channel.
func (cc *copierClient) processMsg() {
	resCache := make(map[int64]chan clientWrappedMsg)
	for {
		select {
		case <-cc.ctx.Done():
			return
		case msg := <-cc.msgIn:
			resCache[msg.msg.GetID()] = msg.resChan
			logger.Log.Debug("processMsg forwarding", "type", msg.msg.Type(), "id", msg.msg.GetID())
			cc.sendHandle <- msg.clientWrappedMsg
		case wmsg := <-cc.receiveHandle:
			key := wmsg.msg.GetID()
			logger.Log.Debug("processMsg got response", "type", wmsg.msg.Type(), "id", key)
			if ch, ok := resCache[key]; ok {
				logger.Log.Debug("processMsg matching response to waiter", "id", key)
				ch <- wmsg
				delete(resCache, key)
			}
		}
	}

}

// dialAndAuth performs the full SSH challenge-response auth on a temporary
// connection to obtain a session token, then spawns batch connection goroutines
// that re-auth with just the token.
func (cc *copierClient) dialAndAuth() error {
	signer, pubKey, err := clientSigner(cc.identityFile)
	if err != nil {
		return fmt.Errorf("auth: %w", err)
	}

	conn, err := net.Dial("tcp", cc.target)
	if err != nil {
		return fmt.Errorf("dial %s: %w", cc.target, err)
	}
	defer conn.Close()

	pubKeyBytes := pubKey.Marshal()

	// Step 1: send public key, receive challenge
	if err := message.WriteMessage(conn, &message.AuthReq{
		ID:     1,
		User:   cc.authUser,
		PubKey: pubKeyBytes,
	}, nil); err != nil {
		return fmt.Errorf("auth send pubkey: %w", err)
	}

	resp, err := readResp(conn)
	if err != nil {
		return fmt.Errorf("auth recv challenge: %w", err)
	}
	challengeRes, ok := resp.(*message.AuthRes)
	if !ok || challengeRes.Success {
		return fmt.Errorf("unexpected auth response")
	}

	// Step 2: sign the challenge and send back
	challenge := challengeRes.Challenge
	sig, err := signer.Sign(randReader, challenge)
	if err != nil {
		return fmt.Errorf("auth sign challenge: %w", err)
	}
	sigBytes := ssh.Marshal(sig)

	if err := message.WriteMessage(conn, &message.AuthReq{
		ID:        2,
		User:      cc.authUser,
		PubKey:    pubKeyBytes,
		Signature: sigBytes,
	}, nil); err != nil {
		return fmt.Errorf("auth send signature: %w", err)
	}

	resp, err = readResp(conn)
	if err != nil {
		return fmt.Errorf("auth recv result: %w", err)
	}
	authRes, ok := resp.(*message.AuthRes)
	if !ok || !authRes.Success {
		errMsg := "auth denied"
		if ok && authRes.Error != "" {
			errMsg = authRes.Error
		}
		return fmt.Errorf("auth: %s", errMsg)
	}

	cc.authToken = authRes.Token
	cc.authUser = authRes.User
	cc.encryptionKey = deriveEncryptionKey(pubKey)

	for i := 0; i < cc.batch; i++ {
		go cc.runConn()
	}
	return nil
}

// runConn maintains one connection in the pool with automatic reconnection.
// Each new connection performs a token-based auth before joining the pool.
func (cc *copierClient) runConn() {
	backoff := 100 * time.Millisecond
	const maxBackoff = 5 * time.Second
	for {
		conn, err := net.Dial("tcp", cc.target)
		if err != nil {
			logger.Log.Debug("reconnect dial error", "err", err)
			select {
			case <-cc.ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
			continue
		}
		backoff = 100 * time.Millisecond

		// Token auth before joining the pool
		if err := cc.tokenAuth(conn); err != nil {
			logger.Log.Debug("token auth failed, closing connection", "err", err)
			conn.Close()
			continue
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			cc.handleSend(conn)
		}()
		go func() {
			defer wg.Done()
			cc.handleReceive(conn)
		}()
		wg.Wait()

		if cc.ctx.Err() != nil {
			return
		}
	}
}

// tokenAuth performs a fast session-token authentication on an already-dialed
// connection. It sends AuthReq{Token} and expects AuthRes{Success: true}.
func (cc *copierClient) tokenAuth(conn net.Conn) error {
	if err := message.WriteMessage(conn, &message.AuthReq{
		ID:    0,
		Token: cc.authToken,
	}, nil); err != nil {
		return err
	}
	resp, err := readResp(conn)
	if err != nil {
		return err
	}
	authRes, ok := resp.(*message.AuthRes)
	if !ok || !authRes.Success {
		return fmt.Errorf("token auth rejected")
	}
	return nil
}

// readResp reads one complete protocol frame from a net.Conn. It is used
// for synchronous auth exchanges before the channel-based pipeline starts.
func readResp(conn net.Conn) (message.MSG, error) {
	head := make([]byte, message.HeadSize)
	if _, err := io.ReadFull(conn, head); err != nil {
		return nil, err
	}
	if !message.MagicNumberCheck(head[0], head[1]) {
		return nil, message.ErrBadProtocol
	}
	msg, msgSize, payloadSize, err := message.DecodePre(head)
	if err != nil {
		return nil, err
	}
	body := make([]byte, msgSize)
	if _, err := io.ReadFull(conn, body); err != nil {
		return nil, err
	}
	if err := msg.Decode(body); err != nil {
		return nil, err
	}
	if payloadSize > 0 {
		if _, err := io.CopyN(io.Discard, conn, int64(payloadSize)); err != nil {
			return nil, err
		}
	}
	return msg, nil
}

// handleSend
//
// encode received messages
// send out to target host by connection
func (cc *copierClient) handleSend(conn net.Conn) {
	defer conn.Close()
	for {
		select {
		case <-cc.ctx.Done():
			return
		case wmsg := <-cc.sendHandle:
			msg := wmsg.msg
			payload := wmsg.payload
			logger.Log.Debug("sending msg", "type", msg.Type(), "id", msg.GetID(), "payloadLen", len(payload))
			head, msgbs := message.EncodeMsg(msg, len(payload))
			if cc.timeout > 0 {
				conn.SetWriteDeadline(time.Now().Add(cc.timeout))
			}
			bufs := net.Buffers{head, msgbs, payload}
			if _, err := bufs.WriteTo(conn); err != nil {
				logger.Log.Debug("write error", "err", err)
				return
			}
			logger.Log.Debug("msg sent", "type", msg.Type(), "id", msg.GetID())
		}
	}
}

// handleReceive is a read-loop state machine that reassembles gcp frames
// from a raw TCP connection. The state transitions are:
//
//  1. readSize < HeadSize        → reading header bytes into bufHead
//  2. len(bufMsg) == 0           → header parsed, allocate bufMsg and payload
//  3. reading CBOR message body  → reading into bufMsg
//  4. reading payload bytes      → reading into payload
//
// After a complete frame is assembled, the message is decoded (DecodePre +
// Decode) and dispatched to receiveHandle. The readSize, bufMsg, payload,
// and magicNumChecked variables track the machine's state across Read calls.
func (cc *copierClient) handleReceive(conn net.Conn) {
	defer conn.Close()
	bufHead := make([]byte, message.HeadSize)
	readSize := 0
	var bufMsg []byte
	var payload []byte
	var magicNumChecked bool
	var buf []byte
	for {
		select {
		case <-cc.ctx.Done():
			return
		default:
			if readSize < message.HeadSize {
				buf = bufHead[readSize:]
				if cc.timeout > 0 {
					conn.SetReadDeadline(time.Now().Add(cc.timeout))
				}
				n, err := conn.Read(buf)
				if err != nil {
					logger.Log.Debug("read error", "err", err)
					return
				}
				readSize += n
				if readSize > 1 && !magicNumChecked {
					if !message.MagicNumberCheck(bufHead[0], bufHead[1]) {
						logger.Log.Debug("bad protocol")
						return
					} else {
						magicNumChecked = true
					}
				}
			} else if len(bufMsg) == 0 {
				msgSize := binary.BigEndian.Uint32(bufHead[3 : 3+message.MessageSize])
				payloadSize := binary.BigEndian.Uint32(bufHead[3+message.MessageSize:])
				if msgSize > message.MaxMsgSize || payloadSize > message.MaxPayloadSize {
					logger.Log.Debug("bad protocol: oversized message", "msgSize", msgSize, "payloadSize", payloadSize)
					return
				}
				bufMsg = make([]byte, msgSize)
				payload = make([]byte, payloadSize)
			} else if readSize < message.HeadSize+len(bufMsg) {
				if cc.timeout > 0 {
					conn.SetReadDeadline(time.Now().Add(cc.timeout))
				}
				n, err := conn.Read(bufMsg[readSize-message.HeadSize:])
				if err != nil {
					logger.Log.Debug("read error", "err", err)
					return
				}
				readSize += n
			} else if readSize < message.HeadSize+len(bufMsg)+len(payload) {
				if cc.timeout > 0 {
					conn.SetReadDeadline(time.Now().Add(cc.timeout))
				}
				n, err := conn.Read(payload[readSize-message.HeadSize-len(bufMsg):])
				if err != nil {
					logger.Log.Debug("read error", "err", err)
					return
				}
				readSize += n
			} else {
				msg, _, _, err := message.DecodePre(bufHead)
				if err != nil {
					logger.Log.Debug("decode error", "err", err)
					return
				}
				if err := msg.Decode(bufMsg); err != nil {
					logger.Log.Debug("decode error", "err", err)
					return
				}
				logger.Log.Debug("received response", "type", msg.Type(), "id", msg.GetID())
				cc.receiveHandle <- clientWrappedMsg{
					msg:     msg,
					payload: append([]byte{}, payload...),
				}
				readSize = 0
				bufMsg = nil
				payload = nil
			}
		}
	}
}

func (cc *copierClient) Close() {
	cc.cancel()
}

// genMsgID returns a monotonically increasing message ID. The counter is
// atomically incremented, making it safe for concurrent callers.
func (cc *copierClient) genMsgID() int64 {
	return atomic.AddInt64(&cc.id, 1)
}

// Create sends a CreateReq and blocks until the response arrives or the
// context is cancelled. This implements an RPC-like synchronous pattern
// over the multiplexed connection pool.
func (cc *copierClient) Create(target string, size int64, mode fs.FileMode) (clientWrappedMsg, error) {
	ch := make(chan clientWrappedMsg)
	cc.msgIn <- clientRequestMsg{
		clientWrappedMsg: clientWrappedMsg{
			msg: &message.CreateReq{
				ID:   cc.genMsgID(),
				Size: size,
				Mode: uint32(mode),
				Path: target,
			},
		},
		resChan: ch,
	}
	logger.Log.Debug("Create waiting for response")
	select {
	case res := <-ch:
		logger.Log.Debug("Create got response")
		return res, nil
	case <-cc.ctx.Done():
		logger.Log.Debug("Create context done")
		return clientWrappedMsg{}, cc.ctx.Err()
	}
}

func (cc *copierClient) Write(target string, off int64, payload []byte, compressionAlgo uint8) (clientWrappedMsg, error) {
	ch := make(chan clientWrappedMsg)

	compressed, algo, err := compressChunk(payload, compressionAlgo)
	if err != nil {
		return clientWrappedMsg{}, err
	}

	var (
		toSend  []byte
		encAlgo uint8
	)
	if cc.useEncryption {
		encrypted, err := encryptChunk(compressed, cc.encryptionKey)
		if err != nil {
			return clientWrappedMsg{}, err
		}
		toSend = encrypted
		encAlgo = message.EncryptionSecretBox
	} else {
		toSend = compressed
		encAlgo = message.EncryptionNone
	}

	req := &message.WriteReq{
		ID:          cc.genMsgID(),
		Offset:      off,
		Path:        target,
		Compression: algo,
		Encryption:  encAlgo,
	}
	if cc.useChecksum && len(toSend) > 0 {
		req.Checksum = crc32.ChecksumIEEE(toSend)
	}
	cc.msgIn <- clientRequestMsg{
		clientWrappedMsg: clientWrappedMsg{
			msg:     req,
			payload: toSend,
		},
		resChan: ch,
	}
	select {
	case res := <-ch:
		return res, nil
	case <-cc.ctx.Done():
		return clientWrappedMsg{}, cc.ctx.Err()
	}
}

func (cc *copierClient) Read(target string, off int64, size int64, compressionAlgo uint8) (clientWrappedMsg, error) {
	ch := make(chan clientWrappedMsg)
	var encAlgo uint8 = message.EncryptionNone
	if cc.useEncryption {
		encAlgo = message.EncryptionSecretBox
	}
	cc.msgIn <- clientRequestMsg{
		clientWrappedMsg: clientWrappedMsg{
			msg: &message.ReadReq{
				ID:          cc.genMsgID(),
				Offset:      off,
				Size:        size,
				Path:        target,
				Compression: compressionAlgo,
				Encryption:  encAlgo,
			},
		},
		resChan: ch,
	}
	select {
	case res := <-ch:
		return res, nil
	case <-cc.ctx.Done():
		return clientWrappedMsg{}, cc.ctx.Err()
	}
}

func (cc *copierClient) ReadDir(target string) (clientWrappedMsg, error) {
	ch := make(chan clientWrappedMsg)
	cc.msgIn <- clientRequestMsg{
		clientWrappedMsg: clientWrappedMsg{
			msg: &message.ReadDirReq{
				ID:   cc.genMsgID(),
				Path: target,
			},
		},
		resChan: ch,
	}
	select {
	case res := <-ch:
		return res, nil
	case <-cc.ctx.Done():
		return clientWrappedMsg{}, cc.ctx.Err()
	}
}

func (cc *copierClient) Stat(target string) (clientWrappedMsg, error) {
	ch := make(chan clientWrappedMsg)
	cc.msgIn <- clientRequestMsg{
		clientWrappedMsg: clientWrappedMsg{
			msg: &message.StatReq{
				ID:   cc.genMsgID(),
				Path: target,
			},
		},
		resChan: ch,
	}
	select {
	case res := <-ch:
		return res, nil
	case <-cc.ctx.Done():
		return clientWrappedMsg{}, cc.ctx.Err()
	}
}

func (cc *copierClient) Hash(target string) (clientWrappedMsg, error) {
	ch := make(chan clientWrappedMsg)
	cc.msgIn <- clientRequestMsg{
		clientWrappedMsg: clientWrappedMsg{
			msg: &message.HashReq{
				ID:   cc.genMsgID(),
				Path: target,
			},
		},
		resChan: ch,
	}
	select {
	case res := <-ch:
		return res, nil
	case <-cc.ctx.Done():
		return clientWrappedMsg{}, cc.ctx.Err()
	}
}
