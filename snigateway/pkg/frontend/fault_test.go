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

package frontend

import (
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type packetItem struct {
	data      []byte
	addr      net.Addr
	deliverAt time.Time
}

// simulatedPacketConn wraps a net.PacketConn and simulates propagation latency and packet loss.
type simulatedPacketConn struct {
	net.PacketConn
	delay     time.Duration
	lossRate  float64
	sendQueue chan packetItem
	recvQueue chan packetItem
	closed    chan struct{}
	closeOnce sync.Once
	dropped   atomic.Int64
}

// newSimulatedPacketConn returns a net.PacketConn wrapping pc with injected one-way delay and loss.
func newSimulatedPacketConn(pc net.PacketConn, delay time.Duration, lossRate float64) *simulatedPacketConn {
	s := &simulatedPacketConn{
		PacketConn: pc,
		delay:      delay,
		lossRate:   lossRate,
		sendQueue:  make(chan packetItem, 65536),
		recvQueue:  make(chan packetItem, 65536),
		closed:     make(chan struct{}),
	}

	if delay > 0 {
		go s.sendLoop()
		go s.recvLoop()
	}

	return s
}

func (s *simulatedPacketConn) sendLoop() {
	for {
		select {
		case <-s.closed:
			return
		case item := <-s.sendQueue:
			now := time.Now()
			if now.Before(item.deliverAt) {
				time.Sleep(item.deliverAt.Sub(now))
			}
			_, _ = s.PacketConn.WriteTo(item.data, item.addr)
		}
	}
}

func (s *simulatedPacketConn) recvLoop() {
	buf := make([]byte, 65536)
	for {
		n, addr, err := s.PacketConn.ReadFrom(buf)
		if err != nil {
			return
		}
		if s.lossRate > 0 && rand.Float64() < s.lossRate {
			s.dropped.Add(1)
			continue
		}
		data := make([]byte, n)
		copy(data, buf[:n])
		item := packetItem{
			data:      data,
			addr:      addr,
			deliverAt: time.Now().Add(s.delay),
		}
		select {
		case s.recvQueue <- item:
		case <-s.closed:
			return
		}
	}
}

func (s *simulatedPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	select {
	case <-s.closed:
		return 0, net.ErrClosed
	default:
	}

	if s.lossRate > 0 && rand.Float64() < s.lossRate {
		s.dropped.Add(1)
		return len(p), nil
	}

	if s.delay <= 0 {
		return s.PacketConn.WriteTo(p, addr)
	}

	data := make([]byte, len(p))
	copy(data, p)
	item := packetItem{
		data:      data,
		addr:      addr,
		deliverAt: time.Now().Add(s.delay),
	}

	select {
	case s.sendQueue <- item:
		return len(p), nil
	case <-s.closed:
		return 0, net.ErrClosed
	default:
		// Drop on queue overflow
		s.dropped.Add(1)
		return len(p), nil
	}
}

func (s *simulatedPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	if s.delay <= 0 {
		for {
			n, addr, err := s.PacketConn.ReadFrom(p)
			if err != nil {
				return n, addr, err
			}
			if s.lossRate > 0 && rand.Float64() < s.lossRate {
				s.dropped.Add(1)
				continue
			}
			return n, addr, nil
		}
	}

	select {
	case <-s.closed:
		return 0, nil, net.ErrClosed
	case item := <-s.recvQueue:
		now := time.Now()
		if now.Before(item.deliverAt) {
			time.Sleep(item.deliverAt.Sub(now))
		}
		n := copy(p, item.data)
		return n, item.addr, nil
	}
}

func (s *simulatedPacketConn) Close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
	})
	return s.PacketConn.Close()
}

// simulatedConn wraps net.Conn to simulate round-trip latency and loss on a byte stream.
type simulatedConn struct {
	net.Conn
	delay     time.Duration
	lossRate  float64
	lastWrite time.Time
	mu        sync.Mutex
}

func newSimulatedConn(c net.Conn, delay time.Duration, lossRate float64) *simulatedConn {
	return &simulatedConn{
		Conn:     c,
		delay:    delay,
		lossRate: lossRate,
	}
}

func (c *simulatedConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	c.lastWrite = time.Now()
	c.mu.Unlock()
	return c.Conn.Write(b)
}

func (c *simulatedConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	lastW := c.lastWrite
	c.lastWrite = time.Time{}
	c.mu.Unlock()

	if !lastW.IsZero() && c.delay > 0 {
		// Delay the arrival of the response after a write by 2 * delay (full RTT)
		elapsed := time.Since(lastW)
		rtt := 2 * c.delay
		if elapsed < rtt {
			time.Sleep(rtt - elapsed)
		}
		// Simulate TCP packet loss retransmission penalty (RTO of ~1 RTT)
		if c.lossRate > 0 && rand.Float64() < c.lossRate {
			time.Sleep(rtt)
		}
	}

	return c.Conn.Read(b)
}

func TestSimulatedPacketConn(t *testing.T) {
	pc1, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}
	defer pc1.Close()

	pc2, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ListenPacket: %v", err)
	}
	defer pc2.Close()

	// 10ms one-way delay (20ms RTT)
	sim1 := newSimulatedPacketConn(pc1, 10*time.Millisecond, 0)
	defer sim1.Close()

	t0 := time.Now()
	_, err = sim1.WriteTo([]byte("ping"), pc2.LocalAddr())
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	buf := make([]byte, 1024)
	n, _, err := pc2.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	elapsed := time.Since(t0)

	if string(buf[:n]) != "ping" {
		t.Fatalf("expected 'ping', got %q", string(buf[:n]))
	}
	if elapsed < 8*time.Millisecond {
		t.Fatalf("expected delay at least ~8ms, got %v", elapsed)
	}
}

func TestSimulatedConn(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 1024)
		n, _ := c.Read(buf)
		_, _ = c.Write(buf[:n])
	}()

	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer raw.Close()

	// 10ms one-way delay (20ms RTT)
	sim := newSimulatedConn(raw, 10*time.Millisecond, 0)

	t0 := time.Now()
	_, err = sim.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	buf := make([]byte, 1024)
	n, err := sim.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	elapsed := time.Since(t0)

	if string(buf[:n]) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(buf[:n]))
	}
	if elapsed < 18*time.Millisecond {
		t.Fatalf("expected RTT delay at least ~18ms, got %v", elapsed)
	}
}
