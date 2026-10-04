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
	"net"
	"testing"
)

func TestProxyProtocolV2_IPv4(t *testing.T) {
	src := &net.TCPAddr{
		IP:   net.ParseIP("192.0.2.1"),
		Port: 12345,
	}
	dst := &net.TCPAddr{
		IP:   net.ParseIP("198.51.100.1"),
		Port: 443,
	}

	orig := &Header{
		Command:   CommandProxy,
		SrcAddr:   src,
		DstAddr:   dst,
		Authority: "echo.example.com",
	}

	data := orig.Format()

	parsed, err := Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if parsed.Version != 2 {
		t.Fatalf("expected version 2, got %d", parsed.Version)
	}
	if parsed.Command != CommandProxy {
		t.Fatalf("expected CommandProxy, got %d", parsed.Command)
	}
	if parsed.SrcAddr.String() != src.String() {
		t.Fatalf("expected SrcAddr %s, got %s", src.String(), parsed.SrcAddr.String())
	}
	if parsed.DstAddr.String() != dst.String() {
		t.Fatalf("expected DstAddr %s, got %s", dst.String(), parsed.DstAddr.String())
	}
	if parsed.Authority != "echo.example.com" {
		t.Fatalf("expected Authority echo.example.com, got %s", parsed.Authority)
	}
}

func TestProxyProtocolV2_IPv6(t *testing.T) {
	src := &net.TCPAddr{
		IP:   net.ParseIP("2001:db8::1"),
		Port: 54321,
	}
	dst := &net.TCPAddr{
		IP:   net.ParseIP("2001:db8::2"),
		Port: 8443,
	}

	orig := &Header{
		Command:   CommandProxy,
		SrcAddr:   src,
		DstAddr:   dst,
		Authority: "ipv6.example.org",
	}

	data := orig.Format()

	parsed, err := Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if parsed.SrcAddr.String() != src.String() {
		t.Fatalf("expected SrcAddr %s, got %s", src.String(), parsed.SrcAddr.String())
	}
	if parsed.DstAddr.String() != dst.String() {
		t.Fatalf("expected DstAddr %s, got %s", dst.String(), parsed.DstAddr.String())
	}
	if parsed.Authority != "ipv6.example.org" {
		t.Fatalf("expected Authority ipv6.example.org, got %s", parsed.Authority)
	}
}

func TestProxyProtocolV2_Unspec(t *testing.T) {
	orig := &Header{
		Command:   CommandProxy,
		Authority: "sni.only.com",
	}

	data := orig.Format()

	parsed, err := Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if parsed.Authority != "sni.only.com" {
		t.Fatalf("expected Authority sni.only.com, got %s", parsed.Authority)
	}
}

func TestProxyProtocolV2_InvalidSignature(t *testing.T) {
	invalid := []byte("INVALID_PROXY_HEADER_1234567890")
	_, err := Decode(bytes.NewReader(invalid))
	if err == nil {
		t.Fatalf("expected error for invalid signature, got nil")
	}
}
