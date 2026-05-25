package asyncio

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
)

const MagicA = 0xA8
const MagicB = 0xD5

type MessageType uint8

const (
	CreateReqT MessageType = iota
	CreateResT
	WriteReqT
	WriteResT
	ReadReqT
	ReadResT
	StatReqT
	StatResT
	ReadDirReqT
	ReadDirResT
)

const MessageSize = 4
const PayloadSize = 4

// 2 bytes for magic numbers
// 1 byte for message type
// 4 bytes for message size
// 4 bytes for payload size
const HeadSize = 2 + 1 + MessageSize + PayloadSize

const MaxMsgSize = 1 << 16
const MaxPayloadSize = 1 << 24

type CreateReq struct {
	ID    int64
	Size  int64
	Mode  uint32
	Path  string
	IsDir bool
}

type CreateRes struct {
	ID      int64
	Success bool
}

type WriteReq struct {
	ID       int64
	Path     string
	Offset   int64
	Checksum uint32
}

type WriteRes struct {
	ID      int64
	Success bool
	N       int32
}

type ReadReq struct {
	ID     int64
	Path   string
	Offset int64
	Size   int64
}

type ReadRes struct {
	ID       int64
	Success  bool
	N        int64
	Checksum uint32
}

type StatReq struct {
	ID   int64
	Path string
}

type StatRes struct {
	ID      int64
	Success bool
	Size    int64
	Mode    uint32
	IsDir   bool
}

type DirEntry struct {
	IsDir bool
	Mode  uint32
	Name  string
}

type ReadDirReq struct {
	ID   int64
	Path string
}

type ReadDirRes struct {
	ID      int64
	Success bool
	Entries []DirEntry
}

// PacketReader is the interface that wraps Peek and Discard,
// matching the signature of gnet.Conn for zero-copy read.
type PacketReader interface {
	Peek(n int) ([]byte, error)
	Discard(n int) (int, error)
}

var (
	ErrIncompletePacket = errors.New("incomplete packet")
	ErrBadProtocol      = errors.New("bad protocol")
	ErrMsgType          = errors.New("unrecognized message type")
	ErrHeadSize         = errors.New("short head size")
)

func ReadMessage(r PacketReader) (MSG, []byte, error) {
	buf, err := r.Peek(2)
	if err != nil {
		return nil, nil, switchError(err)
	}
	if !MagicNumberCheck(buf[0], buf[1]) {
		return nil, nil, ErrBadProtocol
	}
	buf, err = r.Peek(HeadSize)
	if err != nil {
		return nil, nil, switchError(err)
	}
	msg, msgSize, payloadSize, err := DecodePre(buf)
	if err != nil {
		return nil, nil, err
	}
	total := int(msgSize) + int(payloadSize)
	buf, err = r.Peek(HeadSize + total)
	if err != nil {
		return nil, nil, switchError(err)
	}
	if err = msg.Decode(buf[HeadSize : HeadSize+int(msgSize)]); err != nil {
		return nil, nil, err
	}
	payload := make([]byte, payloadSize)
	copy(payload, buf[HeadSize+int(msgSize):])
	r.Discard(len(buf))
	return msg, payload, nil
}

func WriteMessage(w io.Writer, msg MSG, payload []byte) error {
	head, msgbs := EncodeMsg(msg, len(payload))
	bufs := net.Buffers{head, msgbs, payload}
	_, err := bufs.WriteTo(w)
	return err
}

func switchError(err error) error {
	if errors.Is(err, io.ErrShortBuffer) {
		return ErrIncompletePacket
	}
	return err
}

// MagicNumberCheck
//
// Return true if both magic number are match
func MagicNumberCheck(ma byte, mb byte) bool {
	return ma == MagicA && mb == MagicB
}

func EncodeMsg(msg MSG, payloadSize int) (head []byte, msgbuf []byte) {
	head = make([]byte, HeadSize)
	head[0] = MagicA
	head[1] = MagicB
	head[2] = byte(msg.Type())
	msgbuf = msg.Encode()
	binary.BigEndian.PutUint32(head[3:3+MessageSize], uint32(len(msgbuf)))
	binary.BigEndian.PutUint32(head[3+MessageSize:], uint32(payloadSize))
	return
}

func DecodePre(head []byte) (MSG, uint32, uint32, error) {
	if len(head) < HeadSize {
		return nil, 0, 0, ErrHeadSize
	}
	msgt := MessageType(head[2])
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
	case ReadReqT:
		msg = &ReadReq{}
	case ReadResT:
		msg = &ReadRes{}
	case StatReqT:
		msg = &StatReq{}
	case StatResT:
		msg = &StatRes{}
	case ReadDirReqT:
		msg = &ReadDirReq{}
	case ReadDirResT:
		msg = &ReadDirRes{}
	default:
		return nil, 0, 0, ErrMsgType
	}
	msgSize := binary.BigEndian.Uint32(head[3 : 3+MessageSize])
	payloadSize := binary.BigEndian.Uint32(head[3+MessageSize:])
	if msgSize > MaxMsgSize || payloadSize > MaxPayloadSize {
		return nil, 0, 0, ErrBadProtocol
	}
	return msg, msgSize, payloadSize, nil
}
