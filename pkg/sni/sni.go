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

package sni

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	recordTypeHandshake = 0x16
	handshakeTypeClient = 0x01
	extensionServerName = 0x0000
	nameTypeHostName    = 0x00
	maxRecordLength     = 16384 + 5
)

// ErrNotTLS is returned when the connection does not start with a TLS handshake record.
var ErrNotTLS = errors.New("not a TLS handshake")

// ErrNoSNI is returned when a valid TLS ClientHello has no Server Name Indication extension.
var ErrNoSNI = errors.New("no SNI extension found in ClientHello")

// PeekedConn wraps a net.Conn and replays peeked bytes before reading from the underlying connection.
type PeekedConn struct {
	net.Conn
	r      io.Reader
	peeked []byte
}

// Read reads from the peeked buffer first, then the underlying connection.
func (c *PeekedConn) Read(b []byte) (int, error) {
	return c.r.Read(b)
}

// PeekedBytes returns a copy of the peeked bytes.
func (c *PeekedConn) PeekedBytes() []byte {
	res := make([]byte, len(c.peeked))
	copy(res, c.peeked)
	return res
}

// RawConn returns the underlying unwrapped connection.
func (c *PeekedConn) RawConn() net.Conn {
	return c.Conn
}

// NewPeekedConn returns a net.Conn that replays the given peeked bytes on subsequent reads.
func NewPeekedConn(conn net.Conn, peeked []byte) *PeekedConn {
	return &PeekedConn{
		Conn:   conn,
		r:      io.MultiReader(bytes.NewReader(peeked), conn),
		peeked: peeked,
	}
}

// SniffSNI reads enough bytes from conn to extract the TLS SNI hostname.
// It returns the SNI hostname, a PeekedConn that preserves all original bytes, and any error encountered.
func SniffSNI(conn net.Conn, readTimeout time.Duration) (string, net.Conn, error) {
	if readTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		defer func() {
			_ = conn.SetReadDeadline(time.Time{})
		}()
	}

	// Read record header: 5 bytes
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", NewPeekedConn(conn, header), fmt.Errorf("reading TLS record header: %w", err)
	}

	if header[0] != recordTypeHandshake {
		return "", NewPeekedConn(conn, header), ErrNotTLS
	}

	recordLen := int(binary.BigEndian.Uint16(header[3:5]))
	if recordLen <= 0 || recordLen > maxRecordLength {
		return "", NewPeekedConn(conn, header), fmt.Errorf("invalid TLS record length: %d", recordLen)
	}

	payload := make([]byte, recordLen)
	if _, err := io.ReadFull(conn, payload); err != nil {
		allPeeked := append(header, payload...)
		return "", NewPeekedConn(conn, allPeeked), fmt.Errorf("reading TLS record payload: %w", err)
	}

	allPeeked := append(header, payload...)
	peekedConn := NewPeekedConn(conn, allPeeked)

	sni, err := ParseSNI(payload)
	if err != nil {
		return "", peekedConn, err
	}

	return sni, peekedConn, nil
}

// ParseSNI parses a TLS handshake record payload and extracts the SNI hostname.
func ParseSNI(payload []byte) (string, error) {
	if len(payload) < 4 {
		return "", fmt.Errorf("payload too short for handshake header")
	}

	if payload[0] != handshakeTypeClient {
		return "", fmt.Errorf("handshake type is %d, expected ClientHello (1)", payload[0])
	}

	handshakeLen := int(payload[1])<<16 | int(payload[2])<<8 | int(payload[3])
	if len(payload) < 4+handshakeLen {
		return "", fmt.Errorf("incomplete handshake message")
	}

	// Client version (2) + Random (32) = 34 bytes
	pos := 4 + 2 + 32
	if pos >= len(payload) {
		return "", fmt.Errorf("truncated ClientHello after random")
	}

	// Session ID
	sessionIDLen := int(payload[pos])
	pos += 1 + sessionIDLen
	if pos+2 > len(payload) {
		return "", fmt.Errorf("truncated ClientHello at session ID")
	}

	// Cipher Suites
	cipherSuitesLen := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
	pos += 2 + cipherSuitesLen
	if pos+1 > len(payload) {
		return "", fmt.Errorf("truncated ClientHello at cipher suites")
	}

	// Compression Methods
	compressionMethodsLen := int(payload[pos])
	pos += 1 + compressionMethodsLen
	if pos == len(payload) {
		// No extensions present
		return "", ErrNoSNI
	}
	if pos+2 > len(payload) {
		return "", fmt.Errorf("truncated ClientHello at compression methods")
	}

	// Extensions
	extensionsLen := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	end := pos + extensionsLen
	if end > len(payload) {
		return "", fmt.Errorf("truncated extensions block")
	}

	for pos+4 <= end {
		extType := binary.BigEndian.Uint16(payload[pos : pos+2])
		extLen := int(binary.BigEndian.Uint16(payload[pos+2 : pos+4]))
		pos += 4

		if pos+extLen > end {
			return "", fmt.Errorf("truncated extension data")
		}

		if extType == extensionServerName {
			extData := payload[pos : pos+extLen]
			if len(extData) < 2 {
				return "", fmt.Errorf("malformed SNI extension")
			}
			listLen := int(binary.BigEndian.Uint16(extData[0:2]))
			if len(extData) < 2+listLen {
				return "", fmt.Errorf("malformed SNI list")
			}
			subPos := 2
			for subPos+3 <= 2+listLen {
				nameType := extData[subPos]
				nameLen := int(binary.BigEndian.Uint16(extData[subPos+1 : subPos+3]))
				subPos += 3
				if subPos+nameLen > len(extData) {
					return "", fmt.Errorf("truncated server name")
				}
				if nameType == nameTypeHostName {
					return string(extData[subPos : subPos+nameLen]), nil
				}
				subPos += nameLen
			}
		}

		pos += extLen
	}

	return "", ErrNoSNI
}
