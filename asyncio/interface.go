package asyncio

type MSG interface {
	Type() MessageType
	Encode() []byte
	Decode(buf []byte) error
}
