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
	rootproxyproto "github.com/gke-labs/gateway-api-reference-implementation/pkg/proxyproto"
)

var (
	SIGV2                 = rootproxyproto.SIGV2
	ErrInvalidSignature   = rootproxyproto.ErrInvalidSignature
	ErrUnsupportedVersion = rootproxyproto.ErrUnsupportedVersion
	ErrTruncated          = rootproxyproto.ErrTruncated
)

const (
	CommandLocal   = rootproxyproto.CommandLocal
	CommandProxy   = rootproxyproto.CommandProxy
	FamilyUnspec   = rootproxyproto.FamilyUnspec
	FamilyIPv4     = rootproxyproto.FamilyIPv4
	FamilyIPv6     = rootproxyproto.FamilyIPv6
	FamilyUnix     = rootproxyproto.FamilyUnix
	ProtocolUnspec = rootproxyproto.ProtocolUnspec
	ProtocolStream = rootproxyproto.ProtocolStream
	ProtocolDgram  = rootproxyproto.ProtocolDgram
	TypeAuthority  = rootproxyproto.TypeAuthority
)

type Header = rootproxyproto.Header
type Conn = rootproxyproto.Conn

var (
	Encode  = rootproxyproto.Encode
	Decode  = rootproxyproto.Decode
	NewConn = rootproxyproto.NewConn
)
