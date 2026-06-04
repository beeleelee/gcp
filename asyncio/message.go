package asyncio

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
)

const MagicA = 0xA8
const MagicB = 0xD5

// MessageType enumerates the kinds of frames in the gcp protocol.
// Encoded as a single byte in the frame header (supports up to 256 types).
type MessageType uint8

const (
	CreateReqT  MessageType = iota // client→server: prepare remote file for writing
	CreateResT                     // server→client: acknowledges file preparation
	WriteReqT                      // client→server: carries a chunk of file data
	WriteResT                      // server→client: confirms bytes written
	ReadReqT                       // client→server: requests a chunk from a remote file
	ReadResT                       // server→client: carries the requested file chunk
	StatReqT                       // client→server: query file metadata
	StatResT                       // server→client: returns file size, mode, type
	ReadDirReqT                    // client→server: list directory entries
	ReadDirResT                    // server→client: returns directory listing
	HashReqT                       // client→server: request SHA-256 of remote file
	HashResT                       // server→client: returns SHA-256 digest
	AuthReqT                       // client→server: SSH key authentication request
	AuthResT                       // server→client: authentication response
)

// MessageSize and PayloadSize are the byte widths of the two length fields
// in the frame header (both stored as big-endian uint32).
const MessageSize = 4
const PayloadSize = 4

// 2 bytes for magic numbers
// 1 byte for message type
// 4 bytes for message size
// 4 bytes for payload size
const HeadSize = 2 + 1 + MessageSize + PayloadSize

// MaxMsgSize caps the CBOR-encoded message body at 64 KB — enough for any
// message type. MaxPayloadSize caps raw file data at 16 MB, supporting chunk
// sizes up to the protocol limit. Both guard against memory exhaustion from
// a malicious or misconfigured peer.
const MaxMsgSize = 1 << 16
const MaxPayloadSize = 1 << 24

// CreateReq asks the server to prepare a remote file (or directory) before
// streaming data into it. Size is the expected final file length (the server
// may pre-allocate), Mode is the permission bits, and IsDir selects between
// os.MkdirAll and os.OpenFile/O_CREATE behaviour on the server.
type CreateReq struct {
	ID    int64
	Size  int64
	Mode  uint32
	Path  string
	IsDir bool
}

// CreateRes is the server's response to a CreateReq. Success indicates whether
// the remote file or directory was prepared. Error contains the reason on
// failure.
type CreateRes struct {
	ID      int64
	Success bool
	Error   string
}

// Compression algorithm constants. Zero value (CompressionNone) means no
// compression — this is also the default for backward compatibility.
const (
	CompressionNone = 0
	CompressionGzip = 1
)

// Encryption algorithm constants. EncryptionSecretBox uses XSalsa20-Poly1305
// (nacl/secretbox) with a random 24-byte nonce prepended to the ciphertext.
// Zero value (EncryptionNone) is accepted for backward compatibility but will
// be rejected by newer servers that require encryption.
const (
	EncryptionNone     = 0
	EncryptionSecretBox = 1
)

// WriteReq carries a chunk of file data to the server. Path identifies the
// remote file (the same one created earlier), Offset is the byte position for
// positional writes via WriteAt, and Checksum is a CRC-32 IEEE of the payload
// (zero when checksums are disabled). Compression indicates how the payload
// is compressed (CompressionNone = uncompressed). Encryption indicates the
// algorithm used to encrypt the payload (EncryptionNone = unencrypted).
type WriteReq struct {
	ID          int64
	Path        string
	Offset      int64
	Checksum    uint32
	Compression uint8
	Encryption  uint8
}

// WriteRes reports the number of bytes the server wrote. N lets the client
// detect partial writes. Error contains the reason on failure.
type WriteRes struct {
	ID      int64
	Success bool
	N       int32
	Error   string
}

// ReadReq asks the server to return a chunk of a remote file. Offset and Size
// define the byte range; the server reads with ReadAt and returns the data in
// the payload of a ReadRes. Compression requests the server to compress the
// response payload (CompressionNone = no compression). Encryption requests
// encryption of the response payload (EncryptionNone = no encryption).
type ReadReq struct {
	ID          int64
	Path        string
	Offset      int64
	Size        int64
	Compression uint8
	Encryption  uint8
}

// ReadRes carries the requested file data in the payload. N is the number of
// bytes actually read (may be less than the requested Size at EOF), and
// Checksum is the CRC-32 IEEE of the payload. Compression indicates the
// algorithm used to compress the payload. Encryption indicates the algorithm
// used to encrypt the payload. Error contains the reason on failure.
type ReadRes struct {
	ID          int64
	Success     bool
	N           int64
	Checksum    uint32
	Compression uint8
	Encryption  uint8
	Error       string
}

// StatReq queries metadata for a remote path.
type StatReq struct {
	ID   int64
	Path string
}

// StatRes returns file metadata: size, permission mode, and whether the path
// is a directory. Error contains the reason on failure.
type StatRes struct {
	ID      int64
	Success bool
	Size    int64
	Mode    uint32
	IsDir   bool
	Error   string
}

// DirEntry represents a single entry in a remote directory listing, analogous
// to os.FileInfo.
type DirEntry struct {
	IsDir   bool
	Mode    uint32
	Name    string
	Size    int64
	ModTime int64
}

// ReadDirReq asks the server to list the contents of a remote directory.
type ReadDirReq struct {
	ID   int64
	Path string
}

// ReadDirRes returns a list of DirEntry entries from the server. Error
// contains the reason on failure.
type ReadDirRes struct {
	ID      int64
	Success bool
	Entries []DirEntry
	Error   string
}

// HashReq asks the server to compute a SHA-256 digest of a remote file for
// post-transfer integrity verification.
type HashReq struct {
	ID   int64
	Path string
}

// HashRes contains the SHA-256 digest computed by the server. Hash is the
// 32-byte raw digest (not hex-encoded). Error contains the reason on failure.
type HashRes struct {
	ID      int64
	Success bool
	Hash    []byte
	Error   string
}

// AuthReq carries SSH authentication data from the client to the server.
// The first request sends PubKey and optionally a Token for session reuse.
// The server responds with a challenge, which the client signs and sends
// back in a second AuthReq with Signature populated.
type AuthReq struct {
	ID        int64
	User      string // client-claimed username from user@host
	PubKey    []byte // SSH public key wire format (first message)
	Signature []byte // signature of the challenge (second message)
	Token     string // session token for fast re-auth on new connections
}

// AuthRes is the server's response to an AuthReq. On the initial handshake
// it carries a Challenge for the client to sign. On success it returns a
// session Token for subsequent connections and the authenticated User name.
type AuthRes struct {
	ID        int64
	Success   bool
	Challenge []byte // random nonce for the client to sign
	Token     string // session token returned on successful auth
	User      string // authenticated OS username
	Error     string
}

// PacketReader is the interface that wraps Peek and Discard,
// matching the signature of gnet.Conn for zero-copy read.
type PacketReader interface {
	Peek(n int) ([]byte, error)
	Discard(n int) (int, error)
}

var (
	// ErrIncompletePacket is returned by ReadMessage when the reader does not
	// yet have enough data for a complete frame. The caller should wait for
	// more data and retry (not a true error).
	ErrIncompletePacket = errors.New("incomplete packet")
	// ErrBadProtocol indicates corrupted magic bytes or a frame that exceeds
	// MaxMsgSize / MaxPayloadSize.
	ErrBadProtocol = errors.New("bad protocol")
	// ErrMsgType is returned by DecodePre when the type byte does not match
	// any known MessageType.
	ErrMsgType = errors.New("unrecognized message type")
	// ErrHeadSize guards against header reads shorter than HeadSize.
	ErrHeadSize = errors.New("short head size")
	// ErrAuthRequired is returned when a non-auth message is received before
	// the connection has been authenticated.
	ErrAuthRequired = errors.New("authentication required")
)

// ReadMessage reads one complete protocol frame from a PacketReader.
// It peeks the magic bytes, the full header, then the message body and
// payload, decodes the CBOR message, and discards the consumed bytes from
// the reader. The returned payload is a fresh copy independent of the
// reader's internal buffer.
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

// WriteMessage serializes a message and its optional payload into a frame and
// writes it to w. It uses net.Buffers to coalesce the header, CBOR-encoded
// message, and payload into a single writev call.
func WriteMessage(w io.Writer, msg MSG, payload []byte) error {
	head, msgbs := EncodeMsg(msg, len(payload))
	bufs := net.Buffers{head, msgbs, payload}
	_, err := bufs.WriteTo(w)
	return err
}

// switchError converts io.ErrShortBuffer (returned by gnet.Conn.Peek when
// the requested bytes are not yet available) into ErrIncompletePacket so
// that the caller can distinguish "need more data" from real errors.
func switchError(err error) error {
	if errors.Is(err, io.ErrShortBuffer) {
		return ErrIncompletePacket
	}
	return err
}

// MagicNumberCheck reports whether ma and mb match the protocol magic bytes.
func MagicNumberCheck(ma byte, mb byte) bool {
	return ma == MagicA && mb == MagicB
}

// EncodeMsg builds the frame header and CBOR-encodes msg. It returns two
// byte slices: the 11-byte header and the CBOR-encoded message body. The
// caller is responsible for writing the payload separately.
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

// DecodePre reads the fixed-size header from head and returns the appropriate
// MSG type (pre-allocated but not yet decoded), the CBOR message size, and
// the payload size. It validates magic bytes, message type, and size limits.
// The returned MSG still needs its Decode method called with the message body.
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
	case HashReqT:
		msg = &HashReq{}
	case HashResT:
		msg = &HashRes{}
	case AuthReqT:
		msg = &AuthReq{}
	case AuthResT:
		msg = &AuthRes{}
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
