package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"net"
	"sync/atomic"

	"github.com/beeleelee/gcp/asyncio"
)

type clientWrappedMsg struct {
	msg     asyncio.MSG
	payload []byte
}

type clientRequestMsg struct {
	clientWrappedMsg
	resChan chan clientWrappedMsg
}

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
	cc.dail()
	go cc.processMsg()
	return cc
}

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

func (cc *copierClient) dail() {
	for i := 0; i < cc.batch; i++ {
		conn, err := net.Dial("tcp", cc.target)
		if err != nil {
			panic(err)
		}
		go cc.handleSend(conn)
		go cc.handleReceive(conn)
	}
}

func (cc *copierClient) handleSend(conn net.Conn) {
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
				// todo
				// emit connection error
				// handle connection error
			}
			if _, err := conn.Write(msgbs); err != nil {
				// todo
				// emit connection error
				// handle connection error
			}
			if _, err := conn.Write(payload); err != nil {
				// todo
				// emit connection error
				// handle connection error
			}

		}
	}
}

func (cc *copierClient) handleReceive(conn net.Conn) {
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
					fmt.Println(err)
					// conn.Close()
					return
				}
				readSize += n
				if readSize > 1 && !magicNumChecked {
					if bufHead[0] != asyncio.MagicA || bufHead[1] != asyncio.MagicB {
						fmt.Println("bad protocol")
						return
					} else {
						magicNumChecked = true
					}
				}
			}
			if readSize == asyncio.HeadSize {
				msgSize := binary.BigEndian.Uint32(bufHead[3 : 3+asyncio.MessageSize])
				payloadSize := binary.BigEndian.Uint32(bufHead[3+asyncio.MessageSize:])
				bufMsg = make([]byte, msgSize)
				payload = make([]byte, payloadSize)
			}
			if readSize < asyncio.HeadSize+len(bufMsg) {
				n, err := conn.Read(bufMsg[readSize-asyncio.HeadSize:])
				if err != nil {
					fmt.Println(err)
					return
				}
				readSize += n
			} else if readSize < asyncio.HeadSize+len(bufMsg)+len(payload) {
				n, err := conn.Read(payload[readSize-asyncio.HeadSize-len(bufMsg):])
				if err != nil {
					fmt.Println(err)
					return
				}
				readSize += n
			} else {
				// a full packet has been read
				var msg asyncio.MSG
				msgt := asyncio.MessageType(bufHead[2])
				switch msgt {
				case asyncio.CreateReqT:
					msg = &asyncio.CreateReq{}
				case asyncio.CreateResT:
					msg = &asyncio.CreateRes{}
				case asyncio.WriteReqT:
					msg = &asyncio.WriteReq{}
				case asyncio.WriteResT:
					msg = &asyncio.WriteRes{}
				default:
					panic(asyncio.ErrMsgType)
				}
				if err := msg.Decode(bufMsg); err != nil {
					// todo
					// handle error
				}
				cc.receiveHandle <- clientWrappedMsg{
					msg:     msg,
					payload: append([]byte{}, payload...),
				}
				readSize = 0
			}
			// if readSize == asyncio.HeadSize+len(bufMsg)+len(payload) {

			// }
		}
	}
}

func (cc *copierClient) genMsgID() int64 {
	return atomic.AddInt64(&cc.id, 1)
}

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
	return <-ch, nil
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
	return <-ch, nil
}
