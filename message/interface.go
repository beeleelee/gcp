// Package message defines the wire protocol for gcp file transfers.
//
// Each message on the wire is a frame:
//
//	┌──────────┬──────────┬────────────┬──────────────────────┬──────────────────────────┐
//	│ Magic(2) │ Type(1)  │ MsgSize(4) │ PayloadSize(4)      │ Msg bytes  │ Payload      │
//	│ 0xA8 0xD5│          │ (BE)       │ (BE)                 │ (CBOR)     │ (raw bytes)  │
//	└──────────┴──────────┴────────────┴──────────────────────┴──────────────────────────┘
//
// HeadSize = 11 bytes. MaxMsgSize = 64 KB, MaxPayloadSize = 16 MB.
package message

// MSG is the interface every protocol message must implement.
// Each message type carries fields specific to its role (Create, Write, Read,
// Stat, ReadDir, Hash) and the request/response pairs are correlated by
// a caller-assigned ID.
type MSG interface {
	Type() MessageType
	GetID() int64
	Encode() []byte
	Decode(buf []byte) error
}
