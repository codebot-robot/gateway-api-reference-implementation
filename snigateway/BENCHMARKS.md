# SNIGateway Tunnel Overhead Benchmarks: TCP Pool vs QUIC

This document records the benchmarking methodology, empirical measurements, CPU profile analysis, optimizations, and architectural recommendations for SNIGateway's reverse tunnel transports (TCP Pool and QUIC), addressing [Issue #621](https://github.com/gke-labs/gateway-api-reference-implementation/issues/621).

---

## 1. Executive Summary & Transport Recommendations

### Architectural Decision: `auto` Mode Stays QUIC-First
The primary architectural advantage of QUIC reverse tunnels is **initial-connection latency**:
- **0-RTT/1-RTT Stream Establishment**: A new client stream is multiplexed over an established QUIC connection with zero additional connection-dialing round trips and requires **no pre-dialed pool**.
- **Resilience to Connection Bursts**: When traffic bursts exceed the pre-dialed pool size, TCP pool is forced to dial new connections across the WAN, incurring a TCP 3-way handshake + TLS 1.3 handshake penalty (2–3 RTTs). In contrast, QUIC opens new streams instantly.
- **NAT Traversal & Port Economy**: QUIC multiplexes all reverse tunnel traffic across a single UDP 4-tuple, preventing port exhaustion and surviving NAT rebindings.

Therefore, **`auto` mode defaults to QUIC-first**, with TCP pool serving as the automated fallback when UDP is blocked by network firewalls, and as an explicit configuration choice (`--transport=tcp`) for CPU-constrained, line-rate frontends.

### Key Empirical Findings
1. **Connection Burst Latency (WAN Impairment)**: Under a burst of 32 concurrent requests against a pool size of 8:
   - At **20 ms RTT**: QUIC TTFB p50 is **59.3 ms** / p99 is **74.3 ms**, whereas TCP Pool degrades to **156.3 ms** p50 / **253.6 ms** p99 (~3.4x higher latency).
   - At **50 ms RTT**: QUIC TTFB p50 is **137.6 ms** / p99 is **207.6 ms**, whereas TCP Pool degrades to **395.3 ms** p50 / **611.4 ms** p99 (~3.0x higher latency).
2. **Loopback TTFB with Warm Pool**: On loopback with a warm pre-dialed pool, TCP Pool achieves a faster TTFB (**1.28 ms** vs **1.69 ms**). This is expected because loopback hides the 2–3 RTT connection-dialing penalty, and the pre-dialed TCP connection is already waiting in memory.
3. **Frontend CPU per Byte at Saturation**: Under sustained bulk transfer saturation on loopback, the frontend process uses **~3.9–4.1 CPU-seconds per GB** with the **TCP Pool** compared to **~9.9–13.4 CPU-seconds per GB** with **QUIC** (~2.5x to 3.4x higher CPU per byte). Because frontend instances carry aggregated traffic from multiple backends, TCP Pool provides higher bulk throughput per CPU core on line-rate frontend VMs.
4. **Connection Setup Allocations**: QUIC achieves **~45% fewer allocations per connection** (~1,245 allocs/conn) compared to TCP Pool (~2,252 allocs/conn).

---

## 2. Benchmark Methodology

### Symmetrical Test Conditions
Previous benchmarks compared a hand-rolled raw QUIC loop against an `http.Server` TCP pool backend. The harness in `snigateway/pkg/frontend/benchmark_test.go` evaluates both transports under **identical conditions**:
- A real `http.Server` behind `tls.NewListener` attached to `tunnel.NewManager(c, tunnel.WithTransport(...))` for both QUIC and TCP Pool.
- The same mTLS certificate validation, session registration, PROXY protocol v2 header parsing, and client TLS handshakes.

### Measured Metrics
- **Time to First Byte (TTFB)**: p50 and p99 latency for establishing a new connection and receiving the first response byte (`p50-ms`, `p99-ms`).
- **Single-Stream Throughput**: 10 MB payload transfer rate (`MB/s`).
- **Concurrent Aggregate Throughput**: 8 concurrent streams (`MB/s`).
- **Connection Burst Latency**: 32 concurrent new connections against pool size 8 under RTT delays.
- **CPU Time per GB**: User + System CPU seconds consumed per gigabyte transferred, measured for the frontend and backend processes separately (`CPU-s/GB`).
- **Allocations**: Mallocs per connection (`allocs/conn`) and mallocs per MB (`allocs/MB`).
- **UDP Buffer Sizes**: System `rmem_max` / `wmem_max` status and quic-go socket buffer warnings.

---

## 3. Benchmark Results

Tests run on Linux `amd64` (AMD EPYC 9B14 60-core processor, Go 1.27):

### 3.1 Loopback Saturation Baseline (Comparable Transport Data)

| Metric | QUIC | TCP Pool | Delta / Advantage |
|---|---|---|---|
| **TTFB (p50, warm pool)** | 1.69 ms | 1.28 ms | TCP pool ~24% faster (pool pre-warmed) |
| **TTFB (p99, warm pool)** | 2.58 ms | 2.33 ms | TCP pool ~10% faster (pool pre-warmed) |
| **Connection Setup Allocations** | 1,245 allocs/conn | 2,252 allocs/conn | **QUIC ~45% fewer allocations** |
| **Single-Stream Throughput** | 428 MB/s | 731 MB/s | **TCP pool 1.7x faster** |
| **8-Stream Aggregate Throughput** | 320 MB/s | 3,090 MB/s | **TCP pool 9.6x faster** |
| **Throughput Memory Allocations** | 3,325 allocs/MB | 8.4 allocs/MB | **TCP pool 395x fewer allocations** |
| **Frontend Process CPU** | 13.42 CPU-s/GB | 4.11 CPU-s/GB | **TCP pool ~3.3x lower CPU** |
| **Backend Process CPU** | 6.02 CPU-s/GB | 3.60 CPU-s/GB | **TCP pool ~1.7x lower CPU** |
| **Total Process CPU** | 19.44 CPU-s/GB | 7.71 CPU-s/GB | **TCP pool ~2.5x lower CPU** |

### 3.2 Connection Burst Latency (Burst = 32, Pool Size = 8)

When traffic bursts exceed the warm pool capacity under network RTT, QUIC demonstrates its structural advantage by avoiding connection-dialing round trips:

| Network RTT | Transport | TTFB p50 | TTFB p99 | Comparison |
|---|---|---|---|---|
| **20 ms RTT** | **QUIC** | **59.3 ms** | **74.3 ms** | **QUIC is ~3.4x faster** |
| | TCP Pool | 156.3 ms | 253.6 ms | Pool exhaustion stalls on new dials |
| **50 ms RTT** | **QUIC** | **137.6 ms** | **207.6 ms** | **QUIC is ~3.0x faster** |
| | TCP Pool | 395.3 ms | 611.4 ms | Handshake penalties compound across RTT |

### 3.3 Synthetic In-Process Impairment (QUIC Sensitivity Analysis)

> **Methodological Note on In-Process Impairment**:
> The in-process `simulatedPacketConn` wrapper intercepts packets in user space by wrapping `net.PacketConn`. Because the wrapper hides the underlying `*net.UDPConn`, **quic-go automatically disables Generic Segmentation Offload (GSO), Generic Receive Offload (GRO), and batch read/write syscalls (`recvmmsg`/`WriteMsgUDP`)**.
> Meanwhile, the byte-stream `simulatedConn` wrapper for TCP applies delays only on request-response boundaries and cannot simulate kernel-level packet drops on a byte stream.
> Therefore, in-process synthetic drop rows reflect **QUIC sensitivity without GSO** rather than a cross-transport comparison. Cross-transport impairment should be tested with kernel-level `tc netem`.

| In-Process Scenario | QUIC Throughput | QUIC Total CPU / GB | QUIC Allocs / MB | Notes |
|---|---|---|---|---|
| **RTT 0ms, Loss 0%** | 428.5 MB/s | 9.90 s/GB | 3,325 | Loopback baseline with GSO |
| **RTT 20ms, Loss 0%** | 70.7 MB/s | 26.37 s/GB | 5,945 | Unbuffered BDP constraint (no GSO) |
| **RTT 50ms, Loss 0%** | 30.8 MB/s | 26.85 s/GB | 6,004 | Unbuffered BDP constraint (no GSO) |
| **RTT 20ms, Loss 1%** | 0.87 MB/s* | 67.39 s/GB | 5,648 | Drops with default 208 kiB socket buffer |
| **RTT 50ms, Loss 1%** | 0.37 MB/s* | 75.39 s/GB | 5,753 | Drops with default 208 kiB socket buffer |

*\*Note on UDP Buffer Sizes*: Under packet loss, quic-go's recovery mechanisms require raised OS socket buffers (`rmem_max` = 7 MiB). Under default Linux container limits (208 kiB), packet loss triggers kernel socket buffer drops.

#### Recommended Kernel-Level Impairment Testing with `tc netem`
To evaluate both transports under identical kernel-level delay and packet loss without disabling NIC offloads or GSO:
```bash
# Apply 20ms delay and 1% loss to loopback interface
sudo tc qdisc add dev lo root netem delay 20ms loss 1%

# Run benchmark suite
go test -run=^$ -bench='BenchmarkThroughput_' ./pkg/frontend

# Remove impairment
sudo tc qdisc del dev lo root
```

---

## 4. CPU Profile Analysis (`cpu.pprof`) at Saturation

CPU profiling with `go tool pprof` during 10 MB saturation transfers reveals the internal bottlenecks of both transports:

### QUIC Transport Hotspots
1. **Linux Syscalls (`linux.Syscall6` 40.6%)**:
   - `UDPConn.WriteMsgUDP` / `sendPacketsWithGSO`: GSO segment transmission.
   - `oobConn.ReadPacket` / `recvmmsg`: reading incoming UDP datagrams in batches.
2. **Single-Threaded Event Loop (`quic-go.(*Conn).run` 29.4%)**:
   - Each `quic.Conn` runs a single event-loop goroutine managing flow control, timers, ACK generation, and stream dispatch. Under high stream throughput, this single goroutine saturates one CPU core.
3. **Userspace Crypto (`crypto/internal/fips140/aes/gcm` ~12%)**:
   - QUIC packets are individually sealed and decrypted with AES-GCM in userspace Go (1.2–1.45 KB per packet).
4. **Copy Path (`io.CopyBuffer` ~16%)**:
   - Splicing QUIC stream bytes into the client TCP socket.

### TCP Pool Transport Hotspots
1. **Syscalls (`linux.Syscall6` 63.4%)**:
   - Kernel TCP `read` and `write` syscalls. Kernel handles packetization, ACKs, retransmissions, and flow control.
2. **TLS Crypto (`crypto/tls.(*Conn).readFromUntil` ~18%)**:
   - Go standard library TLS 1.3 bulk record decryption.
3. **Copy Path (`io.CopyBuffer` ~8%)**:
   - Splicing between the pooled TLS tunnel connection and the client connection.

---

## 5. System UDP Buffer Limits (`rmem_max` / `wmem_max`)

During QUIC benchmarks, quic-go logs the following warning:
```
failed to sufficiently increase receive buffer size (was: 208 kiB, wanted: 7168 kiB, got: 416 kiB).
See https://github.com/quic-go/quic-go/wiki/UDP-Buffer-Sizes for details.
```

### Analysis & Mitigation
- The default Linux maximum socket receive/send buffer (`net.core.rmem_max` and `net.core.wmem_max`) is 212,992 bytes (208 kiB).
- quic-go requests up to 7,168 kiB (7 MiB) to maintain line-rate throughput over high Bandwidth-Delay Product (BDP) connections and absorb packet bursts.
- On Linux hosts running `snigateway` in production, raise socket limits:
```bash
sudo sysctl -w net.core.rmem_max=7500000
sudo sysctl -w net.core.wmem_max=7500000
```

---

## 6. Optimizations Implemented & Before/After Numbers

### 6.1 Buffer Pooling and 64 KiB Chunks for `spliceConnections`
- **Before**: `spliceConnections` called standard `io.Copy`, which allocated a fresh 32 KiB buffer on every connection in both directions and fell back to 32 KiB `net.genericReadFrom`.
- **After**: Replaced with `io.CopyBuffer` using a `sync.Pool` of 64 KiB byte buffers.
- **Numbers (TCP Pool Throughput on Loopback)**:
  - **Before**: 717.9 MB/s, 3.95 CPU-s/GB, 32 KiB heap allocation per splice.
  - **After**: 731.3 MB/s, 3.93 CPU-s/GB, **0 heap allocations per splice**.

### 6.2 Elimination of `io.MultiReader` Overhead
- **Before**: `proxyproto.Conn` and `sni.PeekedConn` wrapped underlying connections with `io.MultiReader(bytes.NewReader(peeked), conn)`. Even after the peeked bytes were drained, every subsequent read throughout the connection lifetime suffered `io.MultiReader` interface dispatch. Profiling showed this consumed **6.7% of total CPU time**.
- **After**: Implemented direct slice offset draining (`c.buffered[c.bufOffset:]`). Once initial bytes are consumed, `Read()` calls `c.Conn.Read()` directly with zero indirection. Also implemented `io.WriterTo` and `io.ReaderFrom` on `proxyproto.Conn`.
- **Numbers (TCP Pool Throughput on Loopback)**:
  - Eliminated the 6.7% `PeekedConn.Read` CPU hotspot.
  - Retained low memory allocation of 8.4 allocs/MB during bulk transfer.

### 6.3 Elimination of Perpetual `bufio.Reader` in `bufferedConn`
- **Before**: HTTP connection hijacking in `handleConnectionUpgrade` and client tunnel dialing wrapped `rawConn` with a perpetual `bufio.Reader` if handshake bytes were buffered.
- **After**: Drain buffered handshake bytes into a byte slice on upgrade with proper error checking, then read directly from `rawConn`.

### 6.4 Flow-Control Window Configuration
- Preserved standard flow-control window sizes (2 MB initial / 8 MB max stream window, 4 MB initial / 16 MB max conn window) to prevent memory ballooning on high-concurrency multi-tenant frontends.

---

## 7. Open Architectural Discussion: TLS Redundancy vs Metadata Security

### The Redundancy Question
In SNIGateway, client traffic is already end-to-end TLS between the client and the backend pod. Tunneling this traffic over mTLS (either TCP pool TLS or QUIC TLS) means the payload is encrypted twice in userspace.

### Security Trade-Offs
1. **Authentication & Authorization**: The reverse tunnel connection must be mutually authenticated (mTLS) so the frontend can verify client identity (cluster/backend identity and CA allowlist).
2. **Metadata Confidentiality**: The PROXY protocol v2 header carries sensitive connection metadata:
   - Client source IP and port.
   - Target SNI hostname and cluster authority TLVs.
   If the tunnel were unencrypted (integrity-only), this metadata would transit the network in plaintext.
3. **Transport Security Standards**: TLS 1.3 does not define null-cipher authentication modes. Standard TLS ciphers (`TLS_AES_128_GCM_SHA256`) utilize hardware AES-NI instructions and provide guaranteed end-to-end security without custom cryptographic protocols.

### Conclusion
Maintain standard mTLS encryption for both TCP pool and QUIC tunnels. Tunnel overhead should be mitigated through userspace buffer pooling and transport selection rather than compromising metadata confidentiality.
