package asyncio

type MSG interface {
	Type() MessageType
	GetID() int64
	Encode() []byte
	Decode(buf []byte) error
}
