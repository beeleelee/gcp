package asyncio

const MagicA = 0xA8
const MagicB = 0xD5

const (
	CreateReqT uint8 = iota
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
	Path string
	Mode uint32
}

type CreateRes struct {
	Success bool
}

type WriteReq struct {
	Path   string
	Offset int64
	Data   []byte
}

type WriteRes struct {
	Success bool
	N       int32
}
