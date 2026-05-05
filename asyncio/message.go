package asyncio

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/panjf2000/gnet/v2"
)

const MagicA = 0xA8
const MagicB = 0xD5

type MessageType uint8

const (
	CreateReqT MessageType = iota
	CreateResT
	WriteReqT
	WriteResT
)

const MessageSize = 4
const PayloadSize = 4

// 2 bytes for magic numbers
// 1 byte for message type
// 4 bytes for message size
// 4 bytes for payload size
const HeadSize = 2 + 1 + MessageSize + PayloadSize

type CreateReq struct {
	ID   int64
	Path string
	Mode uint32
}

type CreateRes struct {
	ID      int64
	Success bool
}

type WriteReq struct {
	ID     int64
	Path   string
	Offset int64
	Data   []byte
}

type WriteRes struct {
	ID      int64
	Success bool
	N       int32
}

var ErrIncompletePacket = errors.New("incomplete packet")
var ErrBadProtocol = errors.New("bad protocol")
var ErrMsgType = errors.New("unrecognized message type")

func ReadMessage(conn gnet.Conn) (MSG, []byte, error) {
	buf, err := conn.Peek(2)
	if err != nil {
		return nil, nil, switchError(err)
	}
	if buf[0] != MagicA || buf[1] != MagicB {
		return nil, nil, ErrBadProtocol
	}
	buf, err = conn.Peek(HeadSize)
	if err != nil {
		return nil, nil, switchError(err)
	}
	msgt := MessageType(buf[2])
	var msg MSG
	switch msgt {
	case CreateReqT:
		msg = &CreateReq{}
	case CreateResT:
		msg = &CreateRes{}
	case WriteReqT:
		msg = &WriteReq{}
	case WriteResT:
		msg = &WriteRes{}
	default:
		return nil, nil, ErrMsgType
	}
	msgSize := binary.BigEndian.Uint32(buf[3 : 3+MessageSize])
	payloadSize := binary.BigEndian.Uint32(buf[3+MessageSize:])
	buf, err = conn.Peek(HeadSize + int(msgSize+payloadSize))
	if err != nil {
		return nil, nil, switchError(err)
	}
	if err = msg.Decode(buf[HeadSize : HeadSize+msgSize]); err != nil {
		return nil, nil, err
	}
	playload := make([]byte, payloadSize)
	copy(playload, buf[HeadSize+msgSize:])
	return msg, playload, nil
}

func switchError(err error) error {
	if errors.Is(err, io.ErrShortBuffer) {
		return ErrIncompletePacket
	}
	return err
}
