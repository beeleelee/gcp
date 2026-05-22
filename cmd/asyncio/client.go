package main

import (
	"context"
	"encoding/binary"
	"io/fs"
	"net"
	"sync/atomic"

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
	target        string
	id            int64
	batch         int
	msgIn         chan clientRequestMsg
	sendHandle    chan clientWrappedMsg
	receiveHandle chan clientWrappedMsg
}

func newClient(ctx context.Context, target string, batch int) *copierClient {
	cc := &copierClient{
		ctx:           ctx,
		target:        target,
		batch:         batch,
		msgIn:         make(chan clientRequestMsg),
		sendHandle:    make(chan clientWrappedMsg),
		receiveHandle: make(chan clientWrappedMsg),
	}
	cc.dial()
	go cc.processMsg()
	return cc
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
			cc.sendHandle <- msg.clientWrappedMsg
		case wmsg := <-cc.receiveHandle:
			key := wmsg.msg.GetID()
			if ch, ok := resCache[key]; ok {
				ch <- wmsg
				delete(resCache, key)
			}
		}
	}

}

// request and hold connection with target host
// set handles for read and write on connections
func (cc *copierClient) dial() {
	for i := 0; i < cc.batch; i++ {
		conn, err := net.Dial("tcp", cc.target)
		if err != nil {
			panic(err)
		}
		go cc.handleSend(conn)
		go cc.handleReceive(conn)
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
			head := make([]byte, asyncio.HeadSize)
			head[0] = asyncio.MagicA
			head[1] = asyncio.MagicB
			head[2] = byte(msg.Type())
			msgbs := msg.Encode()
			binary.BigEndian.PutUint32(head[3:3+asyncio.MessageSize], uint32(len(msgbs)))
			binary.BigEndian.PutUint32(head[3+asyncio.MessageSize:], uint32(len(payload)))
			if _, err := conn.Write(head); err != nil {
				logger.Log.Debug("write error", "err", err)
				return
			}
			if _, err := conn.Write(msgbs); err != nil {
				logger.Log.Debug("write error", "err", err)
				return
			}
			if _, err := conn.Write(payload); err != nil {
				logger.Log.Debug("write error", "err", err)
				return
			}
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
			} else if readSize == asyncio.HeadSize {
				msgSize := binary.BigEndian.Uint32(bufHead[3 : 3+asyncio.MessageSize])
				payloadSize := binary.BigEndian.Uint32(bufHead[3+asyncio.MessageSize:])
				if msgSize > asyncio.MaxMsgSize || payloadSize > asyncio.MaxPayloadSize {
					logger.Log.Debug("bad protocol: oversized message", "msgSize", msgSize, "payloadSize", payloadSize)
					return
				}
				bufMsg = make([]byte, msgSize)
				payload = make([]byte, payloadSize)
			} else if readSize < asyncio.HeadSize+len(bufMsg) {
				n, err := conn.Read(bufMsg[readSize-asyncio.HeadSize:])
				if err != nil {
					logger.Log.Debug("read error", "err", err)
					return
				}
				readSize += n
			} else if readSize < asyncio.HeadSize+len(bufMsg)+len(payload) {
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
				cc.receiveHandle <- clientWrappedMsg{
					msg:     msg,
					payload: append([]byte{}, payload...),
				}
				readSize = 0
			}
		}
	}
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
	select {
	case res := <-ch:
		return res, nil
	case <-cc.ctx.Done():
		return clientWrappedMsg{}, cc.ctx.Err()
	}
}

func (cc *copierClient) Write(target string, off int64, payload []byte) (clientWrappedMsg, error) {
	ch := make(chan clientWrappedMsg)
	cc.msgIn <- clientRequestMsg{
		clientWrappedMsg: clientWrappedMsg{
			msg: &asyncio.WriteReq{
				ID:     cc.genMsgID(),
				Offset: off,
				Path:   target,
			},
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
