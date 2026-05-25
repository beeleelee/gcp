package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/fs"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beeleelee/gcp/asyncio"
	"github.com/beeleelee/gcp/logger"
)

// wrap asyinc message with payload
// payload contains data bytes or be empty slice
type clientWrappedMsg struct {
	msg     asyncio.MSG
	payload []byte
}

// wrap response channel with clientWrappedMsg
// resChan will receive response msg from server
type clientRequestMsg struct {
	clientWrappedMsg
	resChan chan clientWrappedMsg
}

// copierClinet
// target - the host address to connect to
// id - auto increased int64 value for message matching
// batch - the number of connections going to hold with target host
// msgIn - channel that is the entry point for outside message
// sendHandle - channel that receive messages from processMsg() and send to target host
// receiveHandle - channel that send messages to processMsg() from connections
type copierClient struct {
	ctx           context.Context
	cancel        context.CancelFunc
	target        string
	id            int64
	batch         int
	timeout       time.Duration
	useChecksum   bool
	msgIn         chan clientRequestMsg
	sendHandle    chan clientWrappedMsg
	receiveHandle chan clientWrappedMsg
}

func newClient(ctx context.Context, target string, batch int, timeout time.Duration, useChecksum bool) (*copierClient, error) {
	cc := &copierClient{
		ctx:           ctx,
		target:        target,
		batch:         batch,
		timeout:       timeout,
		useChecksum:   useChecksum,
		msgIn:         make(chan clientRequestMsg),
		sendHandle:    make(chan clientWrappedMsg),
		receiveHandle: make(chan clientWrappedMsg),
	}
	cc.ctx, cc.cancel = context.WithCancel(ctx)
	if err := cc.dial(); err != nil {
		cc.cancel()
		return nil, err
	}
	go cc.processMsg()
	return cc, nil
}

// processMsg
//
// messages match center - match messages with message id
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

// dial probes one initial connection (fail-fast),
// then spawns batch supervised goroutines with reconnect.
func (cc *copierClient) dial() error {
	conn, err := net.Dial("tcp", cc.target)
	if err != nil {
		return fmt.Errorf("dial %s: %w", cc.target, err)
	}
	conn.Close()
	for i := 0; i < cc.batch; i++ {
		go cc.runConn()
	}
	return nil
}

// runConn maintains one connection in the pool with automatic reconnection.
// On I/O error it closes and re-dials with exponential backoff.
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
			head, msgbs := asyncio.EncodeMsg(msg, len(payload))
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

// handleReceive
//
// reading loop on connection
// keeping read packets from target host
// decode packets to message
// transfer message to match center (processMsg())
func (cc *copierClient) handleReceive(conn net.Conn) {
	defer conn.Close()
	bufHead := make([]byte, asyncio.HeadSize)
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
			if readSize < asyncio.HeadSize {
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
					if !asyncio.MagicNumberCheck(bufHead[0], bufHead[1]) {
						logger.Log.Debug("bad protocol")
						return
					} else {
						magicNumChecked = true
					}
				}
			} else if len(bufMsg) == 0 {
				msgSize := binary.BigEndian.Uint32(bufHead[3 : 3+asyncio.MessageSize])
				payloadSize := binary.BigEndian.Uint32(bufHead[3+asyncio.MessageSize:])
				if msgSize > asyncio.MaxMsgSize || payloadSize > asyncio.MaxPayloadSize {
					logger.Log.Debug("bad protocol: oversized message", "msgSize", msgSize, "payloadSize", payloadSize)
					return
				}
				bufMsg = make([]byte, msgSize)
				payload = make([]byte, payloadSize)
			} else if readSize < asyncio.HeadSize+len(bufMsg) {
				if cc.timeout > 0 {
					conn.SetReadDeadline(time.Now().Add(cc.timeout))
				}
				n, err := conn.Read(bufMsg[readSize-asyncio.HeadSize:])
				if err != nil {
					logger.Log.Debug("read error", "err", err)
					return
				}
				readSize += n
			} else if readSize < asyncio.HeadSize+len(bufMsg)+len(payload) {
				if cc.timeout > 0 {
					conn.SetReadDeadline(time.Now().Add(cc.timeout))
				}
				n, err := conn.Read(payload[readSize-asyncio.HeadSize-len(bufMsg):])
				if err != nil {
					logger.Log.Debug("read error", "err", err)
					return
				}
				readSize += n
			} else {
				msg, _, _, err := asyncio.DecodePre(bufHead)
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

// auto increased int64 value
func (cc *copierClient) genMsgID() int64 {
	return atomic.AddInt64(&cc.id, 1)
}

// Create
//
// kind of rpc method
// create file quest to target host
func (cc *copierClient) Create(target string, size int64, mode fs.FileMode) (clientWrappedMsg, error) {
	ch := make(chan clientWrappedMsg)
	cc.msgIn <- clientRequestMsg{
		clientWrappedMsg: clientWrappedMsg{
			msg: &asyncio.CreateReq{
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

func (cc *copierClient) Write(target string, off int64, payload []byte) (clientWrappedMsg, error) {
	ch := make(chan clientWrappedMsg)
	req := &asyncio.WriteReq{
		ID:     cc.genMsgID(),
		Offset: off,
		Path:   target,
	}
	if cc.useChecksum && len(payload) > 0 {
		req.Checksum = crc32.ChecksumIEEE(payload)
	}
	cc.msgIn <- clientRequestMsg{
		clientWrappedMsg: clientWrappedMsg{
			msg:     req,
			payload: payload,
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

func (cc *copierClient) Read(target string, off int64, size int64) (clientWrappedMsg, error) {
	ch := make(chan clientWrappedMsg)
	cc.msgIn <- clientRequestMsg{
		clientWrappedMsg: clientWrappedMsg{
			msg: &asyncio.ReadReq{
				ID:     cc.genMsgID(),
				Offset: off,
				Size:   size,
				Path:   target,
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
			msg: &asyncio.ReadDirReq{
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
			msg: &asyncio.StatReq{
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
