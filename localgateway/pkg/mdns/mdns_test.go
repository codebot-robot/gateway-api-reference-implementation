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

package mdns

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
)

var ipAddrType = gatewayv1.IPAddressType

func getOccupiedPort(t *testing.T) (int, net.PacketConn) {
	t.Helper()
	c, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		t.Fatalf("failed to bind test socket: %v", err)
	}
	port := c.LocalAddr().(*net.UDPAddr).Port
	return port, c
}

// inProcessQuerier is an mDNS client for testing on loopback.
type inProcessQuerier struct {
	port int
	conn net.PacketConn
}

func newInProcessQuerier(t *testing.T, port int) *inProcessQuerier {
	t.Helper()
	c, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		t.Fatalf("failed to create in-process querier: %v", err)
	}
	return &inProcessQuerier{port: port, conn: c}
}

func (q *inProcessQuerier) Close() {
	_ = q.conn.Close()
}

func (q *inProcessQuerier) Query(name string, qtype uint16, timeout time.Duration) ([]dns.RR, error) {
	return q.QueryWithAnswers(name, qtype, nil, timeout)
}

func (q *inProcessQuerier) QueryWithAnswers(name string, qtype uint16, knownAnswers []dns.RR, timeout time.Duration) ([]dns.RR, error) {
	m := new(dns.Msg)
	m.Question = []dns.Question{
		{
			Name:   dns.Fqdn(name),
			Qtype:  qtype,
			Qclass: dns.ClassINET, // legacy query from ephemeral port
		},
	}
	m.Answer = knownAnswers
	data, err := m.Pack()
	if err != nil {
		return nil, err
	}

	dst := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: q.port}
	if _, err := q.conn.WriteTo(data, dst); err != nil {
		return nil, err
	}

	_ = q.conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 4096)
	n, _, err := q.conn.ReadFrom(buf)
	if err != nil {
		return nil, err
	}

	resp := new(dns.Msg)
	if err := resp.Unpack(buf[:n]); err != nil {
		return nil, err
	}
	return resp.Answer, nil
}

// TestPortAlreadyTakenFailsStartup occupies a random ephemeral port first and verifies that
// starting a responder on that port fails clearly without ever binding port 5353.
func TestPortAlreadyTakenFailsStartup(t *testing.T) {
	port, blocker := getOccupiedPort(t)
	defer blocker.Close()

	_, err := NewResponder(ResponderOptions{Port: port})
	if err == nil {
		t.Fatalf("expected NewResponder to fail on occupied port %d, but succeeded", port)
	}

	expectedSub := "is already in use (host avahi-daemon must be stopped or bound elsewhere)"
	if !strings.Contains(err.Error(), expectedSub) {
		t.Fatalf("expected error containing %q, got: %v", expectedSub, err)
	}
}

// TestMDNSPublishAndResolveLoopback tests publishing driven by OnGatewaysUpdate on an ephemeral port
// and querying with in-process querier, asserting legacy unicast formatting (TTL<=10, no cache-flush bit).
func TestMDNSPublishAndResolveLoopback(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	gwKey := types.NamespacedName{Namespace: "default", Name: "my-gw"}
	reconcileCh := make(chan types.NamespacedName, 10)

	// Injected PacketConn on an ephemeral port (:0) - never touches real port 5353
	rawConn, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		t.Fatalf("failed to bind ephemeral socket: %v", err)
	}
	defer rawConn.Close()
	ephemeralPort := rawConn.LocalAddr().(*net.UDPAddr).Port

	pub, err := NewPublisher(PublisherOptions{
		Interface:             "lo",
		Port:                  ephemeralPort,
		IPv4Conn:              rawConn,
		DisableIPv6:           true,
		TTL:                   120,
		ProbeInterval:         20 * time.Millisecond,
		ProbeCount:            2,
		ConflictRetryInterval: 100 * time.Millisecond,
		AnnounceInterval:      20 * time.Millisecond,
		EnqueueGateway: func(req types.NamespacedName) {
			reconcileCh <- req
		},
	})
	if err != nil {
		t.Fatalf("failed to create publisher: %v", err)
	}
	defer pub.Close()
	go func() { _ = pub.Start(ctx) }()

	querier := newInProcessQuerier(t, ephemeralPort)
	defer querier.Close()

	hName := gatewayv1.Hostname("grafana.local")
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  gwKey.Namespace,
			Name:       gwKey.Name,
			Generation: 1,
		},
		Spec: gatewayv1.GatewaySpec{
			Listeners: []gatewayv1.Listener{
				{
					Name:     "http",
					Hostname: &hName,
					Port:     80,
					Protocol: gatewayv1.HTTPProtocolType,
				},
			},
		},
		Status: gatewayv1.GatewayStatus{
			Addresses: []gatewayv1.GatewayStatusAddress{
				{Type: &ipAddrType, Value: "127.0.0.1"},
				{Type: &ipAddrType, Value: "::1"},
			},
			Listeners: []gatewayv1.ListenerStatus{
				{
					Name: "http",
					Conditions: []metav1.Condition{
						{
							Type:   string(gatewayv1.ListenerConditionProgrammed),
							Status: metav1.ConditionTrue,
							Reason: string(gatewayv1.ListenerReasonProgrammed),
						},
					},
				},
			},
		},
	}

	// 1. Data plane push (OnGatewaysUpdate) drives publishing
	pub.OnGatewaysUpdate([]*gatewayv1.Gateway{gw})

	// 2. StatusHook reads current publisher state (initially Probing)
	statusCopy := gw.Status.DeepCopy()
	if err := pub.StatusHook(ctx, gw, statusCopy); err != nil {
		t.Fatalf("StatusHook failed: %v", err)
	}
	cond := state.FindCondition(statusCopy.Listeners[0].Conditions, string(ConditionTypePublished))
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != string(ConditionReasonProbing) {
		t.Fatalf("expected Probing condition, got %+v", cond)
	}

	// 3. Wait for probing to complete and EnqueueGateway to be called
	select {
	case req := <-reconcileCh:
		if req != gwKey {
			t.Fatalf("unexpected reconcile key: %v", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for probe completion")
	}

	// 4. Re-evaluating status after probe completion reports Published=True
	if err := pub.StatusHook(ctx, gw, statusCopy); err != nil {
		t.Fatalf("StatusHook failed: %v", err)
	}
	cond = state.FindCondition(statusCopy.Listeners[0].Conditions, string(ConditionTypePublished))
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != string(ConditionReasonPublished) {
		t.Fatalf("expected Published=True, got %+v", cond)
	}

	// 5. In-process querier sends legacy unicast query (source port != responder.port):
	// Assert RFC 6762 §6.7: TTL must be <= 10s and cache-flush bit must NOT be set!
	answersA, err := querier.Query("grafana.local", dns.TypeA, 1*time.Second)
	if err != nil {
		t.Fatalf("query A failed: %v", err)
	}
	if len(answersA) == 0 {
		t.Fatalf("expected A record, got none")
	}
	recA, ok := answersA[0].(*dns.A)
	if !ok || recA.A.String() != "127.0.0.1" {
		t.Fatalf("expected 127.0.0.1, got %v", answersA[0])
	}
	if recA.Hdr.Class&CacheFlushBit != 0 {
		t.Fatalf("RFC 6762 §6.7 violation: legacy unicast response must NOT set cache-flush bit, got class %d", recA.Hdr.Class)
	}
	if recA.Hdr.Ttl > LegacyUnicastMaxTTL {
		t.Fatalf("RFC 6762 §6.7 violation: legacy unicast TTL must be <= %d, got %d", LegacyUnicastMaxTTL, recA.Hdr.Ttl)
	}

	answersAAAA, err := querier.Query("grafana.local", dns.TypeAAAA, 1*time.Second)
	if err != nil {
		t.Fatalf("query AAAA failed: %v", err)
	}
	if len(answersAAAA) == 0 {
		t.Fatalf("expected AAAA record, got none")
	}
	recAAAA, ok := answersAAAA[0].(*dns.AAAA)
	if !ok || !recAAAA.AAAA.Equal(net.ParseIP("::1")) {
		t.Fatalf("expected ::1, got %v", answersAAAA[0])
	}
	if recAAAA.Hdr.Class&CacheFlushBit != 0 {
		t.Fatalf("RFC 6762 §6.7 violation: legacy unicast response must NOT set cache-flush bit, got class %d", recAAAA.Hdr.Class)
	}
}

// TestMDNSRateLimitAndKnownAnswerSuppression tests multicast response rate limiting (RFC 6762 §6.2)
// and known-answer suppression (§7.1).
func TestMDNSRateLimitAndKnownAnswerSuppression(t *testing.T) {
	resp, err := NewResponder(ResponderOptions{
		Ephemeral:   true,
		DisableIPv6: true,
	})
	if err != nil {
		t.Fatalf("NewResponder failed: %v", err)
	}
	defer resp.Close()

	resp.SetRecord(&RecordSet{
		FQDN: "grafana.local.",
		A:    []net.IP{net.ParseIP("192.168.1.100").To4()},
		TTL:  120,
	})

	var tapMu sync.Mutex
	var tappedMsgs []*dns.Msg
	resp.multicastTap = func(msg *dns.Msg) {
		tapMu.Lock()
		defer tapMu.Unlock()
		tappedMsgs = append(tappedMsgs, msg)
	}

	// 1. Simulate standard multicast query (QU bit not set, port=responder.port)
	q := new(dns.Msg)
	q.Question = []dns.Question{
		{Name: "grafana.local.", Qtype: dns.TypeA, Qclass: dns.ClassINET},
	}
	from := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: resp.Port()}

	resp.handleQuery(q, from, false)

	tapMu.Lock()
	count1 := len(tappedMsgs)
	tapMu.Unlock()
	if count1 != 1 {
		t.Fatalf("expected 1 multicast response, got %d", count1)
	}

	// 2. Immediate second query within 1s MUST be suppressed per RFC 6762 §6.2
	resp.handleQuery(q, from, false)

	tapMu.Lock()
	count2 := len(tappedMsgs)
	tapMu.Unlock()
	if count2 != 1 {
		t.Fatalf("RFC 6762 §6.2 violation: second response within 1s was not rate-limited (got %d)", count2)
	}

	// 3. Known-answer suppression (RFC 6762 §7.1): query containing answer with fresh TTL (100s >= 60s)
	qKnown := new(dns.Msg)
	qKnown.Question = []dns.Question{
		{Name: "grafana.local.", Qtype: dns.TypeA, Qclass: dns.ClassINET},
	}
	qKnown.Answer = []dns.RR{
		&dns.A{
			Hdr: dns.RR_Header{
				Name:   "grafana.local.",
				Rrtype: dns.TypeA,
				Class:  dns.ClassINET,
				Ttl:    100, // >= 120/2
			},
			A: net.ParseIP("192.168.1.100").To4(),
		},
	}

	// Reset rate-limit timer by advancing
	resp.mu.Lock()
	delete(resp.lastMulticast, "grafana.local./1")
	resp.mu.Unlock()

	resp.handleQuery(qKnown, from, false)

	tapMu.Lock()
	count3 := len(tappedMsgs)
	tapMu.Unlock()
	if count3 != 1 {
		t.Fatalf("RFC 6762 §7.1 violation: known answer with fresh TTL was not suppressed (got %d responses)", count3)
	}
}

// TestMDNSRemoveSendsGoodbye verifies that removing a published name sends a goodbye record (TTL=0).
func TestMDNSRemoveSendsGoodbye(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	gwKey := types.NamespacedName{Namespace: "default", Name: "my-gw"}
	reconcileCh := make(chan types.NamespacedName, 10)
	goodbyeCh := make(chan *dns.Msg, 10)

	rawConn, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		t.Fatalf("failed to bind ephemeral socket: %v", err)
	}
	defer rawConn.Close()
	ephemeralPort := rawConn.LocalAddr().(*net.UDPAddr).Port

	pub, err := NewPublisher(PublisherOptions{
		Interface:             "lo",
		Port:                  ephemeralPort,
		IPv4Conn:              rawConn,
		DisableIPv6:           true,
		TTL:                   120,
		ProbeInterval:         20 * time.Millisecond,
		ProbeCount:            2,
		ConflictRetryInterval: 100 * time.Millisecond,
		AnnounceInterval:      20 * time.Millisecond,
		MulticastTap: func(msg *dns.Msg) {
			for _, rr := range msg.Answer {
				if strings.EqualFold(rr.Header().Name, "app.local.") && rr.Header().Ttl == 0 {
					goodbyeCh <- msg
				}
			}
		},
		EnqueueGateway: func(req types.NamespacedName) {
			reconcileCh <- req
		},
	})
	if err != nil {
		t.Fatalf("failed to create publisher: %v", err)
	}
	defer pub.Close()
	go func() { _ = pub.Start(ctx) }()

	hName := gatewayv1.Hostname("app.local")
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: gwKey.Namespace, Name: gwKey.Name, Generation: 1},
		Spec: gatewayv1.GatewaySpec{
			Listeners: []gatewayv1.Listener{
				{Name: "http", Hostname: &hName, Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
		Status: gatewayv1.GatewayStatus{
			Addresses: []gatewayv1.GatewayStatusAddress{{Type: &ipAddrType, Value: "127.0.0.1"}},
		},
	}

	// Add via data plane
	pub.OnGatewaysUpdate([]*gatewayv1.Gateway{gw})

	select {
	case <-reconcileCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for probe completion")
	}

	// Remove listener via data plane
	gwRemoved := gw.DeepCopy()
	gwRemoved.Spec.Listeners = nil
	pub.OnGatewaysUpdate([]*gatewayv1.Gateway{gwRemoved})

	select {
	case msg := <-goodbyeCh:
		if len(msg.Answer) == 0 || msg.Answer[0].Header().Ttl != 0 {
			t.Fatalf("expected TTL 0 in goodbye answer, got %+v", msg.Answer)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("expected goodbye packet with TTL=0 for app.local, but none was observed")
	}
}

// TestMDNSConflictStopsClaim verifies that a conflicting responder during probing stops the claim and sets conflict conditions.
func TestMDNSConflictStopsClaim(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	gwKey := types.NamespacedName{Namespace: "default", Name: "conflict-gw"}
	reconcileCh := make(chan types.NamespacedName, 10)

	rawConn, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		t.Fatalf("failed to bind ephemeral socket: %v", err)
	}
	defer rawConn.Close()
	ephemeralPort := rawConn.LocalAddr().(*net.UDPAddr).Port

	pub, err := NewPublisher(PublisherOptions{
		Interface:             "lo",
		Port:                  ephemeralPort,
		IPv4Conn:              rawConn,
		DisableIPv6:           true,
		TTL:                   120,
		ProbeInterval:         50 * time.Millisecond,
		ProbeCount:            3,
		ConflictRetryInterval: 1 * time.Second,
		AnnounceInterval:      50 * time.Millisecond,
		EnqueueGateway: func(req types.NamespacedName) {
			reconcileCh <- req
		},
	})
	if err != nil {
		t.Fatalf("failed to create publisher: %v", err)
	}
	defer pub.Close()
	go func() { _ = pub.Start(ctx) }()

	hName := gatewayv1.Hostname("conflict.local")
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: gwKey.Namespace, Name: gwKey.Name, Generation: 1},
		Spec: gatewayv1.GatewaySpec{
			Listeners: []gatewayv1.Listener{
				{Name: "http", Hostname: &hName, Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
		Status: gatewayv1.GatewayStatus{
			Addresses: []gatewayv1.GatewayStatusAddress{{Type: &ipAddrType, Value: "127.0.0.1"}},
			Listeners: []gatewayv1.ListenerStatus{
				{
					Name: "http",
					Conditions: []metav1.Condition{
						{Type: string(gatewayv1.ListenerConditionProgrammed), Status: metav1.ConditionTrue},
					},
				},
			},
		},
	}

	// Start data plane push
	pub.OnGatewaysUpdate([]*gatewayv1.Gateway{gw})

	// Inject a conflicting response packet for conflict.local directly into the publisher
	conflictingResp := new(dns.Msg)
	conflictingResp.Response = true
	conflictingResp.Authoritative = true
	conflictingResp.Answer = []dns.RR{
		&dns.A{
			Hdr: dns.RR_Header{
				Name:   "conflict.local.",
				Rrtype: dns.TypeA,
				Class:  dns.ClassINET | CacheFlushBit,
				Ttl:    120,
			},
			A: net.ParseIP("10.0.0.99").To4(),
		},
	}
	respData, _ := conflictingResp.Pack()

	sender, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		t.Fatalf("failed to create sender: %v", err)
	}
	defer sender.Close()

	// Send conflicting response to publisher's port
	dst := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: ephemeralPort}
	if _, err := sender.WriteTo(respData, dst); err != nil {
		t.Fatalf("failed to send conflicting response: %v", err)
	}

	// Wait for conflict detection to enqueue reconcile
	select {
	case req := <-reconcileCh:
		if req != gwKey {
			t.Fatalf("unexpected reconcile key: %v", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for conflict detection")
	}

	// Check conditions via StatusHook
	statusCopy := gw.Status.DeepCopy()
	if err := pub.StatusHook(ctx, gw, statusCopy); err != nil {
		t.Fatalf("StatusHook failed: %v", err)
	}

	pubCond := state.FindCondition(statusCopy.Listeners[0].Conditions, string(ConditionTypePublished))
	if pubCond == nil || pubCond.Status != metav1.ConditionFalse || pubCond.Reason != string(ConditionReasonHostnameConflict) {
		t.Fatalf("expected Published=False HostnameConflict, got: %+v", pubCond)
	}

	confCond := state.FindCondition(statusCopy.Listeners[0].Conditions, string(gatewayv1.ListenerConditionConflicted))
	if confCond == nil || confCond.Status != metav1.ConditionTrue || confCond.Reason != string(gatewayv1.ListenerReasonHostnameConflict) {
		t.Fatalf("expected Conflicted=True HostnameConflict, got: %+v", confCond)
	}

	if _, ok := pub.Responder().GetRecord("conflict.local."); ok {
		t.Fatalf("conflict.local should NOT be published in responder")
	}
}

// TestMDNSConflictAfterPublishing verifies that conflicts occurring AFTER publishing (RFC 6762 §9)
// immediately cease answering, transition to StateConflict, and report the condition.
func TestMDNSConflictAfterPublishing(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	gwKey := types.NamespacedName{Namespace: "default", Name: "post-conflict-gw"}
	reconcileCh := make(chan types.NamespacedName, 10)

	rawConn, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		t.Fatalf("failed to bind ephemeral socket: %v", err)
	}
	defer rawConn.Close()
	ephemeralPort := rawConn.LocalAddr().(*net.UDPAddr).Port

	pub, err := NewPublisher(PublisherOptions{
		Interface:             "lo",
		Port:                  ephemeralPort,
		IPv4Conn:              rawConn,
		DisableIPv6:           true,
		TTL:                   120,
		ProbeInterval:         20 * time.Millisecond,
		ProbeCount:            2,
		ConflictRetryInterval: 1 * time.Second,
		AnnounceInterval:      20 * time.Millisecond,
		EnqueueGateway: func(req types.NamespacedName) {
			reconcileCh <- req
		},
	})
	if err != nil {
		t.Fatalf("failed to create publisher: %v", err)
	}
	defer pub.Close()
	go func() { _ = pub.Start(ctx) }()

	hName := gatewayv1.Hostname("active.local")
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: gwKey.Namespace, Name: gwKey.Name, Generation: 1},
		Spec: gatewayv1.GatewaySpec{
			Listeners: []gatewayv1.Listener{
				{Name: "http", Hostname: &hName, Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
		Status: gatewayv1.GatewayStatus{
			Addresses: []gatewayv1.GatewayStatusAddress{{Type: &ipAddrType, Value: "127.0.0.1"}},
			Listeners: []gatewayv1.ListenerStatus{
				{
					Name: "http",
					Conditions: []metav1.Condition{
						{Type: string(gatewayv1.ListenerConditionProgrammed), Status: metav1.ConditionTrue},
					},
				},
			},
		},
	}

	// 1. Initial publish
	pub.OnGatewaysUpdate([]*gatewayv1.Gateway{gw})

	select {
	case <-reconcileCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for claim")
	}

	// Verify currently published
	if _, ok := pub.Responder().GetRecord("active.local."); !ok {
		t.Fatalf("active.local. should be published")
	}

	// 2. Another host on the LAN announces active.local with a different IP address (RFC 6762 §9)
	conflictingResp := new(dns.Msg)
	conflictingResp.Response = true
	conflictingResp.Authoritative = true
	conflictingResp.Answer = []dns.RR{
		&dns.A{
			Hdr: dns.RR_Header{
				Name:   "active.local.",
				Rrtype: dns.TypeA,
				Class:  dns.ClassINET | CacheFlushBit,
				Ttl:    120,
			},
			A: net.ParseIP("10.0.0.55").To4(),
		},
	}
	respData, _ := conflictingResp.Pack()

	sender, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		t.Fatalf("failed to create sender: %v", err)
	}
	defer sender.Close()

	dst := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: ephemeralPort}
	if _, err := sender.WriteTo(respData, dst); err != nil {
		t.Fatalf("failed to send conflicting response: %v", err)
	}

	// 3. RFC 6762 §9: Must trigger reconcile and cease answering
	select {
	case <-reconcileCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for post-publish conflict notification")
	}

	// Verify record was removed from responder
	if _, ok := pub.Responder().GetRecord("active.local."); ok {
		t.Fatalf("RFC 6762 §9 violation: active.local should have ceased answering immediately after conflict")
	}

	// Verify status condition updated
	statusCopy := gw.Status.DeepCopy()
	if err := pub.StatusHook(ctx, gw, statusCopy); err != nil {
		t.Fatalf("StatusHook failed: %v", err)
	}
	pubCond := state.FindCondition(statusCopy.Listeners[0].Conditions, string(ConditionTypePublished))
	if pubCond == nil || pubCond.Status != metav1.ConditionFalse || pubCond.Reason != string(ConditionReasonHostnameConflict) {
		t.Fatalf("expected Published=False HostnameConflict after post-publish conflict, got: %+v", pubCond)
	}
}

// TestMDNSPublishingOnlyCoversValidLocalHostnames verifies only valid single-label .local names are published.
func TestMDNSPublishingOnlyCoversValidLocalHostnames(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	rawConn, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		t.Fatalf("failed to bind ephemeral socket: %v", err)
	}
	defer rawConn.Close()
	ephemeralPort := rawConn.LocalAddr().(*net.UDPAddr).Port

	pub, err := NewPublisher(PublisherOptions{
		Port:        ephemeralPort,
		IPv4Conn:    rawConn,
		Ephemeral:   true,
		DisableIPv6: true,
	})
	if err != nil {
		t.Fatalf("failed to create publisher: %v", err)
	}
	defer pub.Close()
	go func() { _ = pub.Start(ctx) }()

	invalidHostnames := []string{
		"example.com",
		"a.b.local",
		"*.local",
		"",
		"-bad-.local",
	}

	for _, invalid := range invalidHostnames {
		hName := gatewayv1.Hostname(invalid)
		gw := &gatewayv1.Gateway{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "gw", Generation: 1},
			Spec: gatewayv1.GatewaySpec{
				Listeners: []gatewayv1.Listener{
					{Name: "http", Hostname: &hName, Port: 80, Protocol: gatewayv1.HTTPProtocolType},
				},
			},
			Status: gatewayv1.GatewayStatus{
				Addresses: []gatewayv1.GatewayStatusAddress{{Type: &ipAddrType, Value: "127.0.0.1"}},
				Listeners: []gatewayv1.ListenerStatus{
					{
						Name: "http",
						Conditions: []metav1.Condition{
							{Type: string(gatewayv1.ListenerConditionProgrammed), Status: metav1.ConditionTrue},
						},
					},
				},
			},
		}

		pub.OnGatewaysUpdate([]*gatewayv1.Gateway{gw})

		statusCopy := gw.Status.DeepCopy()
		if err := pub.StatusHook(ctx, gw, statusCopy); err != nil {
			t.Fatalf("StatusHook failed for %q: %v", invalid, err)
		}

		cond := state.FindCondition(statusCopy.Listeners[0].Conditions, string(ConditionTypePublished))
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != string(ConditionReasonUnsupportedHostname) {
			t.Fatalf("for hostname %q expected UnsupportedHostname, got: %+v", invalid, cond)
		}

		if _, ok := pub.Responder().GetRecord(invalid); ok {
			t.Fatalf("hostname %q was unexpectedly published in responder", invalid)
		}
	}
}

// TestMDNSGracefulShutdownSendsGoodbye verifies that canceling context on graceful shutdown sends goodbye packets.
func TestMDNSGracefulShutdownSendsGoodbye(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	gwKey := types.NamespacedName{Namespace: "default", Name: "shutdown-gw"}
	reconcileCh := make(chan types.NamespacedName, 10)
	goodbyeCh := make(chan *dns.Msg, 10)

	rawConn, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		t.Fatalf("failed to bind ephemeral socket: %v", err)
	}
	defer rawConn.Close()
	ephemeralPort := rawConn.LocalAddr().(*net.UDPAddr).Port

	pub, err := NewPublisher(PublisherOptions{
		Interface:             "lo",
		Port:                  ephemeralPort,
		IPv4Conn:              rawConn,
		DisableIPv6:           true,
		TTL:                   120,
		ProbeInterval:         20 * time.Millisecond,
		ProbeCount:            2,
		ConflictRetryInterval: 100 * time.Millisecond,
		AnnounceInterval:      20 * time.Millisecond,
		MulticastTap: func(msg *dns.Msg) {
			for _, rr := range msg.Answer {
				if strings.EqualFold(rr.Header().Name, "shutdown.local.") && rr.Header().Ttl == 0 {
					goodbyeCh <- msg
				}
			}
		},
		EnqueueGateway: func(req types.NamespacedName) {
			reconcileCh <- req
		},
	})
	if err != nil {
		t.Fatalf("failed to create publisher: %v", err)
	}
	defer pub.Close()
	go func() { _ = pub.Start(ctx) }()

	hName := gatewayv1.Hostname("shutdown.local")
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: gwKey.Namespace, Name: gwKey.Name, Generation: 1},
		Spec: gatewayv1.GatewaySpec{
			Listeners: []gatewayv1.Listener{
				{Name: "http", Hostname: &hName, Port: 80, Protocol: gatewayv1.HTTPProtocolType},
			},
		},
		Status: gatewayv1.GatewayStatus{
			Addresses: []gatewayv1.GatewayStatusAddress{{Type: &ipAddrType, Value: "127.0.0.1"}},
			Listeners: []gatewayv1.ListenerStatus{
				{
					Name: "http",
					Conditions: []metav1.Condition{
						{Type: string(gatewayv1.ListenerConditionProgrammed), Status: metav1.ConditionTrue},
					},
				},
			},
		},
	}

	pub.OnGatewaysUpdate([]*gatewayv1.Gateway{gw})

	select {
	case <-reconcileCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for probe completion")
	}

	// Trigger graceful shutdown by canceling context
	cancel()

	select {
	case msg := <-goodbyeCh:
		if len(msg.Answer) == 0 || msg.Answer[0].Header().Ttl != 0 {
			t.Fatalf("expected TTL 0 in shutdown goodbye, got %+v", msg.Answer)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("expected goodbye packet with TTL=0 on graceful shutdown, but none was observed")
	}
}
