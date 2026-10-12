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

package api

import (
	"net/http"
	"time"

	"golang.org/x/net/http2"
)

const (
	// DefaultH2PoolSize is the default number of long-lived multiplexed H2 tunnel connections per frontend.
	DefaultH2PoolSize = 4

	// DefaultH2PingInterval is the default interval for sending HTTP/2 PING frames on idle connections.
	DefaultH2PingInterval = 15 * time.Second

	// DefaultH2PingTimeout is the default timeout for receiving an acknowledgment to an HTTP/2 PING frame.
	DefaultH2PingTimeout = 15 * time.Second

	// DefaultH2DrainTimeout is the default duration to allow in-flight HTTP/2 streams to finish during GOAWAY drain.
	DefaultH2DrainTimeout = 30 * time.Second

	// DefaultH2MaxUploadBufferPerConnection is the connection-level flow control receive window (16 MiB).
	DefaultH2MaxUploadBufferPerConnection int32 = 16 * 1024 * 1024

	// DefaultH2MaxUploadBufferPerStream is the stream-level flow control receive window (8 MiB).
	DefaultH2MaxUploadBufferPerStream int32 = 8 * 1024 * 1024

	// DefaultH2MaxReadFrameSize is the maximum frame payload size (1 MiB) for HTTP/2 frames.
	DefaultH2MaxReadFrameSize uint32 = 1024 * 1024

	// DefaultH2MaxConcurrentStreams is the maximum concurrent streams per pooled connection advertised by the backend.
	DefaultH2MaxConcurrentStreams uint32 = 250
)

// DefaultH2Server returns an http2.Server with flow control, buffer sizes, frame sizes,
// and idle ping checks tuned for high-throughput snigateway reverse tunnels.
func DefaultH2Server() *http2.Server {
	return &http2.Server{
		MaxConcurrentStreams:         DefaultH2MaxConcurrentStreams,
		MaxUploadBufferPerConnection: DefaultH2MaxUploadBufferPerConnection,
		MaxUploadBufferPerStream:     DefaultH2MaxUploadBufferPerStream,
		MaxReadFrameSize:             DefaultH2MaxReadFrameSize,
		ReadIdleTimeout:              DefaultH2PingInterval,
		PingTimeout:                  DefaultH2PingTimeout,
	}
}

// DefaultH2Transport returns an http2.Transport with flow control, buffer sizes, frame sizes,
// and idle ping checks tuned for high-throughput snigateway reverse tunnels.
func DefaultH2Transport() *http2.Transport {
	t1 := &http.Transport{
		HTTP2: &http.HTTP2Config{
			MaxReceiveBufferPerConnection: int(DefaultH2MaxUploadBufferPerConnection),
			MaxReceiveBufferPerStream:     int(DefaultH2MaxUploadBufferPerStream),
			MaxReadFrameSize:              int(DefaultH2MaxReadFrameSize),
			SendPingTimeout:               DefaultH2PingInterval,
			PingTimeout:                   DefaultH2PingTimeout,
		},
	}
	tr2, err := http2.ConfigureTransports(t1)
	if err != nil {
		return &http2.Transport{
			AllowHTTP:        true,
			MaxReadFrameSize: DefaultH2MaxReadFrameSize,
			ReadIdleTimeout:  DefaultH2PingInterval,
			PingTimeout:      DefaultH2PingTimeout,
		}
	}
	tr2.AllowHTTP = true
	tr2.MaxReadFrameSize = DefaultH2MaxReadFrameSize
	tr2.ReadIdleTimeout = DefaultH2PingInterval
	tr2.PingTimeout = DefaultH2PingTimeout
	return tr2
}
