package asyncio

import "github.com/fxamacker/cbor/v2"

var _ MSG = (*CreateReq)(nil)
var _ MSG = (*CreateRes)(nil)
var _ MSG = (*WriteReq)(nil)
var _ MSG = (*WriteRes)(nil)

func (m *CreateReq) Type() MessageType {
	return CreateReqT
}

func (m *CreateReq) GetID() int64 {
	return m.ID
}

func (m *CreateReq) Encode() []byte {
	bs, _ := cbor.Marshal(m)
	return bs
}

func (m *CreateReq) Decode(buf []byte) error {
	return cbor.Unmarshal(buf, m)
}

func (m *CreateRes) Type() MessageType {
	return CreateResT
}

func (m *CreateRes) GetID() int64 {
	return m.ID
}

func (m *CreateRes) Encode() []byte {
	bs, _ := cbor.Marshal(m)
	return bs
}

func (m *CreateRes) Decode(buf []byte) error {
	return cbor.Unmarshal(buf, m)
}

func (m *WriteReq) Type() MessageType {
	return WriteReqT
}

func (m *WriteReq) GetID() int64 {
	return m.ID
}

func (m *WriteReq) Encode() []byte {
	bs, _ := cbor.Marshal(m)
	return bs
}

func (m *WriteReq) Decode(buf []byte) error {
	return cbor.Unmarshal(buf, m)
}

func (m *WriteRes) Type() MessageType {
	return WriteResT
}

func (m *WriteRes) GetID() int64 {
	return m.ID
}

func (m *WriteRes) Encode() []byte {
	bs, _ := cbor.Marshal(m)
	return bs
}

func (m *WriteRes) Decode(buf []byte) error {
	return cbor.Unmarshal(buf, m)
}
