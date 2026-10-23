// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxyproto

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

var (
	// SIGV2 is the 12-byte PROXY protocol v2 signature.
	SIGV2 = []byte("\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A")

	// SIGV1 is the prefix for PROXY protocol v1 text lines.
	SIGV1 = []byte("PROXY ")

	// ErrInvalidSignature is returned when the stream does not begin with a valid PROXY signature.
	ErrInvalidSignature = errors.New("invalid proxy protocol signature")

	// ErrUnsupportedVersion is returned when the version is not supported.
	ErrUnsupportedVersion = errors.New("unsupported proxy protocol version")

	// ErrTruncated is returned when the header is shorter than expected.
	ErrTruncated = errors.New("truncated proxy protocol header")

	// ErrBadHeader is returned when the header format or address data is invalid.
	ErrBadHeader = errors.New("malformed proxy protocol header")
)

const (
	// Command constants
	CommandLocal byte = 0x00
	CommandProxy byte = 0x01

	// Address family constants
	FamilyUnspec byte = 0x00
	FamilyIPv4   byte = 0x10
	FamilyIPv6   byte = 0x20
	FamilyUnix   byte = 0x30

	// Transport protocol constants
	ProtocolUnspec byte = 0x00
	ProtocolStream byte = 0x01
	ProtocolDgram  byte = 0x02

	// TLV Types
	TypeAuthority byte = 0x02 // Host name value passed by client (SNI)

	// MaxV1HeaderLen is the maximum allowed length of a PROXY v1 header line including CRLF.
	MaxV1HeaderLen = 107
)

// Header represents a parsed PROXY protocol v1 or v2 header.
type Header struct {
	Version   byte
	Command   byte
	SrcAddr   net.Addr
	DstAddr   net.Addr
	Authority string
}

// Format serializes the Header into PROXY protocol format (v1 or v2 depending on Version).
func (h *Header) Format() ([]byte, error) {
	buf := new(bytes.Buffer)
	if h.Version == 1 {
		if err := EncodeV1(buf, h); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	if err := Encode(buf, h); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Encode writes a PROXY protocol v2 header to w.
func Encode(w io.Writer, h *Header) error {
	var body bytes.Buffer

	cmd := h.Command
	if cmd == 0 {
		cmd = CommandProxy
	}

	famProto := FamilyUnspec | ProtocolUnspec

	srcTCP, srcOK := h.SrcAddr.(*net.TCPAddr)
	dstTCP, dstOK := h.DstAddr.(*net.TCPAddr)

	if srcOK && dstOK && srcTCP != nil && dstTCP != nil {
		src4 := srcTCP.IP.To4()
		dst4 := dstTCP.IP.To4()

		if src4 != nil && dst4 != nil {
			famProto = FamilyIPv4 | ProtocolStream
			body.Write(src4)
			body.Write(dst4)
			if err := binary.Write(&body, binary.BigEndian, uint16(srcTCP.Port)); err != nil {
				return fmt.Errorf("writing source port: %w", err)
			}
			if err := binary.Write(&body, binary.BigEndian, uint16(dstTCP.Port)); err != nil {
				return fmt.Errorf("writing dest port: %w", err)
			}
		} else {
			src16 := srcTCP.IP.To16()
			dst16 := dstTCP.IP.To16()
			if src16 != nil && dst16 != nil {
				famProto = FamilyIPv6 | ProtocolStream
				body.Write(src16)
				body.Write(dst16)
				if err := binary.Write(&body, binary.BigEndian, uint16(srcTCP.Port)); err != nil {
					return fmt.Errorf("writing source port: %w", err)
				}
				if err := binary.Write(&body, binary.BigEndian, uint16(dstTCP.Port)); err != nil {
					return fmt.Errorf("writing dest port: %w", err)
				}
			}
		}
	}

	// Add Authority TLV if present
	if h.Authority != "" {
		authBytes := []byte(h.Authority)
		body.WriteByte(TypeAuthority)
		if err := binary.Write(&body, binary.BigEndian, uint16(len(authBytes))); err != nil {
			return fmt.Errorf("writing authority TLV length: %w", err)
		}
		body.Write(authBytes)
	}

	headerLen := uint16(body.Len())

	// 12 bytes signature
	if _, err := w.Write(SIGV2); err != nil {
		return err
	}

	// byte 13: version 2 (0x20) | command
	// byte 14: family & protocol
	// byte 15-16: length
	fixed := []byte{
		0x20 | (cmd & 0x0F),
		famProto,
		byte(headerLen >> 8),
		byte(headerLen & 0xFF),
	}

	if _, err := w.Write(fixed); err != nil {
		return err
	}

	if headerLen > 0 {
		if _, err := w.Write(body.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

// EncodeV1 writes a PROXY protocol v1 header line to w.
func EncodeV1(w io.Writer, h *Header) error {
	if h.Command == CommandLocal || h.SrcAddr == nil || h.DstAddr == nil {
		_, err := w.Write([]byte("PROXY UNKNOWN\r\n"))
		return err
	}

	srcTCP, srcOK := h.SrcAddr.(*net.TCPAddr)
	dstTCP, dstOK := h.DstAddr.(*net.TCPAddr)
	if !srcOK || !dstOK || srcTCP == nil || dstTCP == nil {
		_, err := w.Write([]byte("PROXY UNKNOWN\r\n"))
		return err
	}

	src4 := srcTCP.IP.To4()
	dst4 := dstTCP.IP.To4()
	if src4 != nil && dst4 != nil {
		line := fmt.Sprintf("PROXY TCP4 %s %s %d %d\r\n", src4.String(), dst4.String(), srcTCP.Port, dstTCP.Port)
		_, err := w.Write([]byte(line))
		return err
	}

	src16 := srcTCP.IP.To16()
	dst16 := dstTCP.IP.To16()
	if src16 != nil && dst16 != nil {
		line := fmt.Sprintf("PROXY TCP6 %s %s %d %d\r\n", src16.String(), dst16.String(), srcTCP.Port, dstTCP.Port)
		_, err := w.Write([]byte(line))
		return err
	}

	_, err := w.Write([]byte("PROXY UNKNOWN\r\n"))
	return err
}

// Decode reads and parses either a PROXY protocol v1 or v2 header from r.
func Decode(r io.Reader) (*Header, error) {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}

	// Peek at the first 12 bytes to determine version
	sig, err := br.Peek(12)
	if err != nil {
		if errors.Is(err, io.EOF) && len(sig) >= 6 && bytes.HasPrefix(sig, SIGV1) {
			// v1 line might be shorter than 12 bytes or buffer has only partial header
			return DecodeV1(br)
		}
		if len(sig) >= len(SIGV1) && bytes.HasPrefix(sig, SIGV1) {
			return DecodeV1(br)
		}
		if len(sig) == 0 {
			return nil, err
		}
		return nil, ErrInvalidSignature
	}

	if bytes.Equal(sig, SIGV2) {
		return DecodeV2(br)
	}

	if bytes.HasPrefix(sig, SIGV1) {
		return DecodeV1(br)
	}

	return nil, ErrInvalidSignature
}

// DecodeV1 reads and parses a PROXY protocol v1 header from r.
func DecodeV1(r io.Reader) (*Header, error) {
	var buf []byte
	b := make([]byte, 1)

	for len(buf) < MaxV1HeaderLen {
		_, err := io.ReadFull(r, b)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, ErrTruncated
			}
			return nil, fmt.Errorf("reading proxy v1 header: %w", err)
		}
		buf = append(buf, b[0])
		if len(buf) >= 2 && buf[len(buf)-2] == '\r' && buf[len(buf)-1] == '\n' {
			break
		}
	}

	if len(buf) < 2 || buf[len(buf)-2] != '\r' || buf[len(buf)-1] != '\n' {
		return nil, ErrBadHeader
	}

	line := string(buf[:len(buf)-2])
	parts := strings.Split(line, " ")
	if len(parts) < 2 || parts[0] != "PROXY" {
		return nil, ErrInvalidSignature
	}

	proto := parts[1]
	if proto == "UNKNOWN" {
		return &Header{
			Version: 1,
			Command: CommandLocal,
		}, nil
	}

	if proto != "TCP4" && proto != "TCP6" {
		return nil, ErrBadHeader
	}

	if len(parts) != 6 {
		return nil, ErrBadHeader
	}

	srcIPStr, dstIPStr := parts[2], parts[3]
	srcPortStr, dstPortStr := parts[4], parts[5]

	srcIP := net.ParseIP(srcIPStr)
	dstIP := net.ParseIP(dstIPStr)
	if srcIP == nil || dstIP == nil {
		return nil, ErrBadHeader
	}

	if proto == "TCP4" {
		if srcIP.To4() == nil || dstIP.To4() == nil {
			return nil, ErrBadHeader
		}
	} else if proto == "TCP6" {
		if srcIP.To4() != nil || dstIP.To4() != nil {
			return nil, ErrBadHeader
		}
	}

	srcPort, err := strconv.Atoi(srcPortStr)
	if err != nil || srcPort < 0 || srcPort > 65535 {
		return nil, ErrBadHeader
	}

	dstPort, err := strconv.Atoi(dstPortStr)
	if err != nil || dstPort < 0 || dstPort > 65535 {
		return nil, ErrBadHeader
	}

	return &Header{
		Version: 1,
		Command: CommandProxy,
		SrcAddr: &net.TCPAddr{IP: srcIP, Port: srcPort},
		DstAddr: &net.TCPAddr{IP: dstIP, Port: dstPort},
	}, nil
}

// DecodeV2 reads and parses a PROXY protocol v2 header from r.
func DecodeV2(r io.Reader) (*Header, error) {
	fixed := make([]byte, 16)
	if _, err := io.ReadFull(r, fixed); err != nil {
		return nil, fmt.Errorf("reading fixed proxy header: %w", err)
	}

	if !bytes.Equal(fixed[:12], SIGV2) {
		return nil, ErrInvalidSignature
	}

	verCmd := fixed[12]
	ver := verCmd >> 4
	if ver != 2 {
		return nil, ErrUnsupportedVersion
	}
	cmd := verCmd & 0x0F

	famProto := fixed[13]
	fam := famProto & 0xF0
	proto := famProto & 0x0F

	length := binary.BigEndian.Uint16(fixed[14:16])
	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("reading proxy payload (%d bytes): %w", length, err)
		}
	}

	h := &Header{
		Version: ver,
		Command: cmd,
	}

	offset := 0

	if fam == FamilyIPv4 && proto == ProtocolStream {
		if len(payload) < 12 {
			return nil, ErrTruncated
		}
		srcIP := net.IPv4(payload[0], payload[1], payload[2], payload[3])
		dstIP := net.IPv4(payload[4], payload[5], payload[6], payload[7])
		srcPort := binary.BigEndian.Uint16(payload[8:10])
		dstPort := binary.BigEndian.Uint16(payload[10:12])
		h.SrcAddr = &net.TCPAddr{IP: srcIP, Port: int(srcPort)}
		h.DstAddr = &net.TCPAddr{IP: dstIP, Port: int(dstPort)}
		offset = 12
	} else if fam == FamilyIPv6 && proto == ProtocolStream {
		if len(payload) < 36 {
			return nil, ErrTruncated
		}
		srcIP := make(net.IP, 16)
		dstIP := make(net.IP, 16)
		copy(srcIP, payload[0:16])
		copy(dstIP, payload[16:32])
		srcPort := binary.BigEndian.Uint16(payload[32:34])
		dstPort := binary.BigEndian.Uint16(payload[34:36])
		h.SrcAddr = &net.TCPAddr{IP: srcIP, Port: int(srcPort)}
		h.DstAddr = &net.TCPAddr{IP: dstIP, Port: int(dstPort)}
		offset = 36
	}

	// Parse TLVs
	for offset+3 <= len(payload) {
		t := payload[offset]
		l := int(binary.BigEndian.Uint16(payload[offset+1 : offset+3]))
		offset += 3
		if offset+l > len(payload) {
			return nil, ErrTruncated
		}
		val := payload[offset : offset+l]
		offset += l

		if t == TypeAuthority {
			h.Authority = string(val)
		}
	}

	return h, nil
}

// Conn wraps a net.Conn with RemoteAddr and LocalAddr extracted from a PROXY header.
type Conn struct {
	net.Conn
	remoteAddr net.Addr
	localAddr  net.Addr
	buffered   []byte
	bufOffset  int
}

// NewConn creates a new Conn wrapping conn.
func NewConn(conn net.Conn, remoteAddr, localAddr net.Addr, buffered []byte) *Conn {
	c := &Conn{
		Conn:       conn,
		remoteAddr: remoteAddr,
		localAddr:  localAddr,
	}
	if len(buffered) > 0 {
		c.buffered = buffered
	}
	return c
}

func (c *Conn) RemoteAddr() net.Addr {
	if c.remoteAddr != nil {
		return c.remoteAddr
	}
	return c.Conn.RemoteAddr()
}

func (c *Conn) LocalAddr() net.Addr {
	if c.localAddr != nil {
		return c.localAddr
	}
	return c.Conn.LocalAddr()
}

func (c *Conn) Read(b []byte) (int, error) {
	if c.buffered != nil {
		n := copy(b, c.buffered[c.bufOffset:])
		c.bufOffset += n
		if c.bufOffset >= len(c.buffered) {
			c.buffered = nil
		}
		return n, nil
	}
	return c.Conn.Read(b)
}

func (c *Conn) WriteTo(w io.Writer) (int64, error) {
	var n int64
	if c.buffered != nil {
		rem := c.buffered[c.bufOffset:]
		c.buffered = nil
		nw, err := w.Write(rem)
		n += int64(nw)
		if err != nil {
			return n, err
		}
	}
	if wt, ok := c.Conn.(io.WriterTo); ok {
		nw, err := wt.WriteTo(w)
		return n + nw, err
	}
	nw, err := io.Copy(w, c.Conn)
	return n + nw, err
}

func (c *Conn) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := c.Conn.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return io.Copy(c.Conn, r)
}
