package asyncio

import "github.com/fxamacker/cbor/v2"

// Compile-time checks: every concrete message type implements MSG.
var _ MSG = (*CreateReq)(nil)
var _ MSG = (*CreateRes)(nil)
var _ MSG = (*WriteReq)(nil)
var _ MSG = (*WriteRes)(nil)
var _ MSG = (*ReadReq)(nil)
var _ MSG = (*ReadRes)(nil)
var _ MSG = (*StatReq)(nil)
var _ MSG = (*StatRes)(nil)
var _ MSG = (*ReadDirReq)(nil)
var _ MSG = (*ReadDirRes)(nil)
var _ MSG = (*HashReq)(nil)
var _ MSG = (*HashRes)(nil)
var _ MSG = (*AuthReq)(nil)
var _ MSG = (*AuthRes)(nil)

func (m *CreateReq) Type() MessageType {
	return CreateReqT
}

func (m *CreateReq) GetID() int64 {
	return m.ID
}

func (m *CreateReq) Encode() []byte {
	// cbor.Marshal cannot fail on simple structs; the error is discarded.
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

func (m *ReadReq) Type() MessageType {
	return ReadReqT
}

func (m *ReadReq) GetID() int64 {
	return m.ID
}

func (m *ReadReq) Encode() []byte {
	bs, _ := cbor.Marshal(m)
	return bs
}

func (m *ReadReq) Decode(buf []byte) error {
	return cbor.Unmarshal(buf, m)
}

func (m *ReadRes) Type() MessageType {
	return ReadResT
}

func (m *ReadRes) GetID() int64 {
	return m.ID
}

func (m *ReadRes) Encode() []byte {
	bs, _ := cbor.Marshal(m)
	return bs
}

func (m *ReadRes) Decode(buf []byte) error {
	return cbor.Unmarshal(buf, m)
}

func (m *StatReq) Type() MessageType {
	return StatReqT
}

func (m *StatReq) GetID() int64 {
	return m.ID
}

func (m *StatReq) Encode() []byte {
	bs, _ := cbor.Marshal(m)
	return bs
}

func (m *StatReq) Decode(buf []byte) error {
	return cbor.Unmarshal(buf, m)
}

func (m *StatRes) Type() MessageType {
	return StatResT
}

func (m *StatRes) GetID() int64 {
	return m.ID
}

func (m *StatRes) Encode() []byte {
	bs, _ := cbor.Marshal(m)
	return bs
}

func (m *StatRes) Decode(buf []byte) error {
	return cbor.Unmarshal(buf, m)
}

func (m *ReadDirReq) Type() MessageType {
	return ReadDirReqT
}

func (m *ReadDirReq) GetID() int64 {
	return m.ID
}

func (m *ReadDirReq) Encode() []byte {
	bs, _ := cbor.Marshal(m)
	return bs
}

func (m *ReadDirReq) Decode(buf []byte) error {
	return cbor.Unmarshal(buf, m)
}

func (m *ReadDirRes) Type() MessageType {
	return ReadDirResT
}

func (m *ReadDirRes) GetID() int64 {
	return m.ID
}

func (m *ReadDirRes) Encode() []byte {
	bs, _ := cbor.Marshal(m)
	return bs
}

func (m *ReadDirRes) Decode(buf []byte) error {
	return cbor.Unmarshal(buf, m)
}

func (m *HashReq) Type() MessageType {
	return HashReqT
}

func (m *HashReq) GetID() int64 {
	return m.ID
}

func (m *HashReq) Encode() []byte {
	bs, _ := cbor.Marshal(m)
	return bs
}

func (m *HashReq) Decode(buf []byte) error {
	return cbor.Unmarshal(buf, m)
}

func (m *HashRes) Type() MessageType {
	return HashResT
}

func (m *HashRes) GetID() int64 {
	return m.ID
}

func (m *HashRes) Encode() []byte {
	bs, _ := cbor.Marshal(m)
	return bs
}

func (m *HashRes) Decode(buf []byte) error {
	return cbor.Unmarshal(buf, m)
}

func (m *AuthReq) Type() MessageType {
	return AuthReqT
}

func (m *AuthReq) GetID() int64 {
	return m.ID
}

func (m *AuthReq) Encode() []byte {
	bs, _ := cbor.Marshal(m)
	return bs
}

func (m *AuthReq) Decode(buf []byte) error {
	return cbor.Unmarshal(buf, m)
}

func (m *AuthRes) Type() MessageType {
	return AuthResT
}

func (m *AuthRes) GetID() int64 {
	return m.ID
}

func (m *AuthRes) Encode() []byte {
	bs, _ := cbor.Marshal(m)
	return bs
}

func (m *AuthRes) Decode(buf []byte) error {
	return cbor.Unmarshal(buf, m)
}
