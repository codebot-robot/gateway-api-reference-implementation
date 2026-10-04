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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

var (
	// SIGV2 is the 12-byte PROXY protocol v2 signature.
	SIGV2 = []byte("\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A")

	// ErrInvalidSignature is returned when the stream does not begin with the v2 signature.
	ErrInvalidSignature = errors.New("invalid proxy protocol v2 signature")

	// ErrUnsupportedVersion is returned when the version is not 2.
	ErrUnsupportedVersion = errors.New("unsupported proxy protocol version")

	// ErrTruncated is returned when the header is shorter than expected.
	ErrTruncated = errors.New("truncated proxy protocol header")
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
)

// Header represents a parsed PROXY protocol v2 header.
type Header struct {
	Version   byte
	Command   byte
	SrcAddr   net.Addr
	DstAddr   net.Addr
	Authority string
}

// Format serializes the Header into PROXY protocol v2 binary format.
func (h *Header) Format() []byte {
	buf := new(bytes.Buffer)
	_ = Encode(buf, h)
	return buf.Bytes()
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
			_ = binary.Write(&body, binary.BigEndian, uint16(srcTCP.Port))
			_ = binary.Write(&body, binary.BigEndian, uint16(dstTCP.Port))
		} else {
			src16 := srcTCP.IP.To16()
			dst16 := dstTCP.IP.To16()
			if src16 != nil && dst16 != nil {
				famProto = FamilyIPv6 | ProtocolStream
				body.Write(src16)
				body.Write(dst16)
				_ = binary.Write(&body, binary.BigEndian, uint16(srcTCP.Port))
				_ = binary.Write(&body, binary.BigEndian, uint16(dstTCP.Port))
			}
		}
	}

	// Add Authority TLV if present
	if h.Authority != "" {
		authBytes := []byte(h.Authority)
		body.WriteByte(TypeAuthority)
		_ = binary.Write(&body, binary.BigEndian, uint16(len(authBytes)))
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

// Decode reads and parses a PROXY protocol v2 header from r.
func Decode(r io.Reader) (*Header, error) {
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
	r          io.Reader
}

// NewConn creates a new Conn wrapping conn.
func NewConn(conn net.Conn, remoteAddr, localAddr net.Addr, buffered []byte) *Conn {
	c := &Conn{
		Conn:       conn,
		remoteAddr: remoteAddr,
		localAddr:  localAddr,
	}
	if len(buffered) > 0 {
		c.r = io.MultiReader(bytes.NewReader(buffered), conn)
	} else {
		c.r = conn
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
	return c.r.Read(b)
}
