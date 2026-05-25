package asyncio

import (
	"encoding/binary"
	"testing"
)

func TestMagicNumberCheck(t *testing.T) {
	tests := []struct {
		a, b byte
		want bool
	}{
		{MagicA, MagicB, true},
		{0, 0, false},
		{MagicA, 0, false},
		{0, MagicB, false},
		{0xA8, 0xD5, true},
	}
	for _, tc := range tests {
		if got := MagicNumberCheck(tc.a, tc.b); got != tc.want {
			t.Errorf("MagicNumberCheck(%#x, %#x) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestEncodeDecodePreRoundTrip(t *testing.T) {
	tests := []struct {
		name        string
		msg         MSG
		payloadSize int
	}{
		{"CreateReq", &CreateReq{ID: 1, Size: 4096, Mode: 0644, Path: "/tmp/f"}, 0},
		{"CreateRes-success", &CreateRes{ID: 2, Success: true}, 0},
		{"CreateRes-fail", &CreateRes{ID: 3, Success: false}, 0},
		{"WriteReq", &WriteReq{ID: 4, Path: "/tmp/f", Offset: 1024}, 32768},
		{"WriteRes-success", &WriteRes{ID: 5, Success: true, N: 4096}, 0},
		{"WriteRes-fail", &WriteRes{ID: 6, Success: false, N: 0}, 0},
		{"ReadReq", &ReadReq{ID: 7, Path: "/tmp/f", Offset: 0, Size: 4096}, 0},
		{"ReadRes", &ReadRes{ID: 8, Success: true, N: 4096, Checksum: 0}, 4096},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			head, msgbs := EncodeMsg(tc.msg, tc.payloadSize)
			if len(head) != HeadSize {
				t.Fatalf("head len = %d, want %d", len(head), HeadSize)
			}
			if !MagicNumberCheck(head[0], head[1]) {
				t.Error("header missing magic bytes")
			}
			if head[2] != byte(tc.msg.Type()) {
				t.Errorf("type byte = %d, want %d", head[2], tc.msg.Type())
			}

			gotMsg, gotMsgSize, gotPayloadSize, err := DecodePre(head)
			if err != nil {
				t.Fatalf("DecodePre: %v", err)
			}
			if gotMsg.Type() != tc.msg.Type() {
				t.Errorf("msg type = %d, want %d", gotMsg.Type(), tc.msg.Type())
			}
			if gotMsgSize != uint32(len(msgbs)) {
				t.Errorf("msg size = %d, want %d", gotMsgSize, len(msgbs))
			}
			if gotPayloadSize != uint32(tc.payloadSize) {
				t.Errorf("payload size = %d, want %d", gotPayloadSize, tc.payloadSize)
			}
			if err := gotMsg.Decode(msgbs); err != nil {
				t.Fatalf("Decode: %v", err)
			}
		})
	}
}

func TestMsgCBORRoundTrip(t *testing.T) {
	req := &CreateReq{ID: 42, Size: 65536, Mode: 0755, Path: "/remote/file.bin"}
	bs := req.Encode()
	var decoded CreateReq
	if err := decoded.Decode(bs); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.ID != req.ID {
		t.Errorf("ID = %d, want %d", decoded.ID, req.ID)
	}
	if decoded.Size != req.Size {
		t.Errorf("Size = %d, want %d", decoded.Size, req.Size)
	}
	if decoded.Mode != req.Mode {
		t.Errorf("Mode = %o, want %o", decoded.Mode, req.Mode)
	}
	if decoded.Path != req.Path {
		t.Errorf("Path = %q, want %q", decoded.Path, req.Path)
	}
	if decoded.IsDir {
		t.Error("IsDir = true, want false")
	}
}

func TestCreateReqIsDirCBORRoundTrip(t *testing.T) {
	req := &CreateReq{ID: 1, Size: 0, Mode: 0755, Path: "/remote/dir", IsDir: true}
	bs := req.Encode()
	var decoded CreateReq
	if err := decoded.Decode(bs); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.ID != req.ID {
		t.Errorf("ID = %d, want %d", decoded.ID, req.ID)
	}
	if !decoded.IsDir {
		t.Error("IsDir = false, want true")
	}
}

func TestWriteReqCBORRoundTrip(t *testing.T) {
	req := &WriteReq{ID: 7, Path: "/remote/f", Offset: 8192}
	bs := req.Encode()
	var decoded WriteReq
	if err := decoded.Decode(bs); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.ID != req.ID ||
		decoded.Path != req.Path ||
		decoded.Offset != req.Offset {
		t.Errorf("round-trip mismatch: %+v vs %+v", decoded, *req)
	}
}

func TestWriteResCBORRoundTrip(t *testing.T) {
	res := &WriteRes{ID: 8, Success: true, N: 4096}
	bs := res.Encode()
	var decoded WriteRes
	if err := decoded.Decode(bs); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded != *res {
		t.Errorf("round-trip mismatch: %+v", decoded)
	}
}

func TestCreateResCBORRoundTrip(t *testing.T) {
	res := &CreateRes{ID: 9, Success: false}
	bs := res.Encode()
	var decoded CreateRes
	if err := decoded.Decode(bs); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded != *res {
		t.Errorf("round-trip mismatch: %+v", decoded)
	}
}

func TestReadReqCBORRoundTrip(t *testing.T) {
	req := &ReadReq{ID: 10, Path: "/remote/f", Offset: 4096, Size: 32768}
	bs := req.Encode()
	var decoded ReadReq
	if err := decoded.Decode(bs); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded != *req {
		t.Errorf("round-trip mismatch: %+v", decoded)
	}
}

func TestReadResCBORRoundTrip(t *testing.T) {
	res := &ReadRes{ID: 11, Success: true, N: 32768, Checksum: 12345678}
	bs := res.Encode()
	var decoded ReadRes
	if err := decoded.Decode(bs); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded != *res {
		t.Errorf("round-trip mismatch: %+v", decoded)
	}
}

func TestEncodeMsgPayloadSizeInHeader(t *testing.T) {
	msg := &WriteReq{ID: 1, Path: "/f", Offset: 0}
	head, _ := EncodeMsg(msg, 65535)
	got := binary.BigEndian.Uint32(head[3+MessageSize:])
	if got != 65535 {
		t.Errorf("payload size in header = %d, want 65535", got)
	}
}

func TestDecodePreShortHead(t *testing.T) {
	_, _, _, err := DecodePre([]byte{0xA8, 0xD5})
	if err != ErrHeadSize {
		t.Errorf("got %v, want %v", err, ErrHeadSize)
	}
}

func TestDecodePreBadMsgType(t *testing.T) {
	buf := make([]byte, HeadSize)
	buf[0] = MagicA
	buf[1] = MagicB
	buf[2] = 99
	_, _, _, err := DecodePre(buf)
	if err != ErrMsgType {
		t.Errorf("got %v, want %v", err, ErrMsgType)
	}
}

func TestGetID(t *testing.T) {
	if id := (&CreateReq{ID: 10}).GetID(); id != 10 {
		t.Errorf("CreateReq.GetID = %d", id)
	}
	if id := (&CreateRes{ID: 11}).GetID(); id != 11 {
		t.Errorf("CreateRes.GetID = %d", id)
	}
	if id := (&WriteReq{ID: 12}).GetID(); id != 12 {
		t.Errorf("WriteReq.GetID = %d", id)
	}
	if id := (&WriteRes{ID: 13}).GetID(); id != 13 {
		t.Errorf("WriteRes.GetID = %d", id)
	}
	if id := (&ReadReq{ID: 14}).GetID(); id != 14 {
		t.Errorf("ReadReq.GetID = %d", id)
	}
	if id := (&ReadRes{ID: 15}).GetID(); id != 15 {
		t.Errorf("ReadRes.GetID = %d", id)
	}
}

func TestTypeConstants(t *testing.T) {
	if CreateReqT != 0 {
		t.Errorf("CreateReqT = %d, want 0", CreateReqT)
	}
	if CreateResT != 1 {
		t.Errorf("CreateResT = %d, want 1", CreateResT)
	}
	if WriteReqT != 2 {
		t.Errorf("WriteReqT = %d, want 2", WriteReqT)
	}
	if WriteResT != 3 {
		t.Errorf("WriteResT = %d, want 3", WriteResT)
	}
	if ReadReqT != 4 {
		t.Errorf("ReadReqT = %d, want 4", ReadReqT)
	}
	if ReadResT != 5 {
		t.Errorf("ReadResT = %d, want 5", ReadResT)
	}
}

func TestHeadSizeConstant(t *testing.T) {
	want := 2 + 1 + 4 + 4
	if HeadSize != want {
		t.Errorf("HeadSize = %d, want %d", HeadSize, want)
	}
}
