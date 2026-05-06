package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"

	"github.com/beeleelee/gcp/asyncio"
)

type copierClient struct {
	ctx    context.Context
	target string
	idChan chan int64
	batch  int
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
			}
			if readSize == asyncio.HeadSize+len(bufMsg)+len(payload) {

			}
		}
	}
}

func (cc *copierClient) genMsgID() {
	var id int64
	for {
		select {
		case <-cc.ctx.Done():
			return
		default:
			id++
			cc.idChan <- id
		}
	}
}
