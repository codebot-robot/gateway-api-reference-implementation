# Experiment: Write-Ahead Log + Buffer Pool Disk Store for Informer Objects

## Executive Summary

This experiment evaluates a custom, pure Go disk-backed store under `experiments/diskstore` designed specifically for Kubernetes informer caches. It replaces the `bbolt` prototype from `experiments/bboltcache` to solve two critical architectural limitations:

1. **The `mmap` Problem:** In `bbolt`, reading disk pages relies on memory-mapped files (`mmap`). Operating system page faults block the underlying OS thread transparently without the Go runtime's knowledge. This blocks Go goroutine scheduling, delays stop-the-world garbage collection pauses, and inflates container memory accounting (Process RSS and cgroup memory limits).
2. **The Durability vs. Performance Dilemma:** In `bbolt`, running with `NoSync: true` can silently serve corrupted or rolled-back data following a kernel writeback error, while running with `NoSync: false` causes synchronous fsync calls on every commit, reducing throughput to single-digit operations per second.

The new `DiskStore` architecture introduces:
- A sequential **Write-Ahead Log (WAL)** using 64-bit monotonic LSNs and immutable segment files with directory fsyncing.
- A **heap file composed of self-describing 4 KiB blocks**, each protected by a 32-byte header with a Castagnoli CRC32C checksum.
- An **out-of-place write strategy** enforcing the **Reuse Rule** (locations holding the newest durable copy of a page are never overwritten) and the **Write-Ahead Rule** (WAL is fsynced through a page's LSN before the page is written to disk).
- A **buddy allocator** over 4 KiB blocks that prefers the lowest available location, enabling automatic tail truncation of the heap file.
- A **fixed-size buffer pool** of 4 KiB frames allocated once as a single pointer-free byte arena with zero `mmap`, performing direct I/O via `O_DIRECT` on Linux (and `F_NOCACHE` on Darwin) through `unix.Preadv` and `unix.Pwritev`.
- **Non-blocking checkpoints** with ARIES dirty page tracking and WAL segment truncation.
- Full compatibility with `k8s.io/client-go/tools/cache.Indexer`.

### Key Empirical Results (Mixed Realistic Corpus: 10,000 Objects, 10 KB Payloads)

*Measured on Linux 6.6 amd64 with direct I/O enabled and an 8 MiB (2,048-frame) buffer pool.*

| Architecture | Heap In-Use | Process RSS | Disk Size | Write Throughput | Recovery Time | Warm Recompute (10 refs) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **In-Memory Informer** (Status Quo) | 115.94 MB | 103.48 MB | 0 MB (RAM) | 1,831,254 op/s | N/A | 11 µs |
| **BBolt Store** (`mmap` + `NoSync`) | 115.33 MB | 301.85 MB | 144.25 MB | 13,767 op/s | 12.0 ms | 9 µs |
| **DiskStore (Uncompressed)** | 114.76 MB | 187.31 MB | 159.90 MB | 2,586 op/s | 555.0 ms | 36 µs |
| **DiskStore (zstd)** | **114.77 MB** | **197.62 MB** | **92.71 MB** | **3,026 op/s** | **350.0 ms** | **62 µs** |
| **DiskStore (S2)** | 114.77 MB | 183.43 MB | 82.73 MB | 3,244 op/s | 296.0 ms | 48 µs |
| **DiskStore (Snappy)** | 114.78 MB | 181.45 MB | 82.61 MB | 3,194 op/s | 310.0 ms | 27 µs |

- **Disk Footprint Reduction by Data Type:**
  - **Configuration text (JSON):** `zstd` reduces on-disk storage by **82.3%** (8.51 MB vs. 47.95 MB uncompressed) and triples write throughput (11,056 op/s vs. 3,631 op/s).
  - **TLS material (x509 PEM):** `zstd` reduces on-disk storage by **66.3%** (16.20 MB vs. 48.01 MB uncompressed).
  - **Random bytes:** Incompressible data triggers the 10% threshold rule and stores raw without compression expansion (47.96 MB across all codecs).
  - **Mixed realistic corpus:** `zstd` achieves a **42.0% disk reduction** (92.71 MB vs. 159.90 MB uncompressed) across a corpus containing one-third incompressible data.
- **RSS Reduction:** `DiskStore (zstd)` consumes **197.62 MB Process RSS**, achieving a **34.5% reduction compared to BBolt** (301.85 MB) by avoiding kernel page-cache amplification from `mmap`.
- **Write Throughput Acceleration:** Compressing values allows objects to fit within single-block slotted pages or smaller multi-block allocations, reducing buffer pool frame eviction churn and issuing fewer write system calls during WAL logging and page flushes.
- **Zero Mmap Overhead:** Because direct I/O bypasses the operating system page cache and buffer pool memory is bounded to a fixed arena, container memory accounting remains flat and predictable under load.
- **Fast Startup & Recovery:** The sequential scan in physical block order reconstructs the store state across the mixed corpus in **350 ms**.

---

## Layered Architecture: Engine and Kubernetes Cache Adapter

To maintain separation of concerns and avoid exposing Kubernetes dependencies to low-level storage routines, the implementation is split into two packages under `experiments/diskstore`:

### 1. Core Database Engine (`experiments/diskstore/engine`)

The engine provides a byte-oriented key-value store with crash consistency, bounded memory usage, and no dependencies on `k8s.io/...`:
- **Data abstraction:** All keys and values are processed as raw byte slices (`[]byte`) partitioned across buckets.
- **Operations:** Supports `Open`, `Put(bucket, key, value, token)`, `Delete(bucket, key, token)`, `Get(bucket, key)`, point-in-time consistent `Scan(bucket, fn)`, `Flush`, `Sync`, `Checkpoint`, and `Close`.
- **Progress tokens:** Write and delete operations accept an opaque string `token` persisted in the WAL record. During crash recovery, the engine tracks the highest token for each bucket and exposes them via `Token(bucket)` and `Tokens()`.
- **Payload encryption:** If a cipher is configured, encryption and decryption occur inside the engine using the bucket and key as authenticated additional data.
- **Sticky error state and notifications:** Once an unrecoverable error occurs (such as an fsync failure or page corruption), the engine transitions permanently into a failed state where all subsequent calls return `ErrFailed`. Callers receive asynchronous notification via `Err()` and `Failed() <-chan struct{}`.
- **Failure marker:** Upon entering the failed state, the engine writes a `FAILED` marker file to the storage directory. If a subsequent `Open` encounters this marker, it wipes the directory, reports the wipe via `Wiped() bool`, and initializes a fresh database with a new ID and no progress tokens.

### 2. Kubernetes Adapter (`experiments/diskstore/kubecache`)

The adapter implements client-go's `cache.Indexer` on top of the engine:
- **Object serialization:** Handles conversion between `runtime.Object` instances and raw bytes using protobuf or JSON codecs (`ObjectCodec`).
- **Secondary indexes:** Maintains index functions and index tables (`Indexers`, `Indices`) in memory, reconstructing them on startup via `engine.Scan`.
- **ResourceVersion tracking:** Maps the Kubernetes `resourceVersion` and watch bookmarks to the engine's per-bucket progress tokens.
- **Fail-fast policy:** Holds the unified error policy required to safely back client-go informer caches with persistent storage.

---

## Fail-Fast Error Policy and Trade-offs

### The Problem with Partial Informer Failures

client-go's cache interfaces are designed around in-memory hash maps that assume storage never fails:
- Read methods such as `List() []any` and `ListKeys() []string` provide no error return in their signatures.
- The reflector logs errors from `Add`, `Update`, and `Delete` via `utilruntime.HandleError` but continues watching without retrying the failed cache write.
- Secondary indexing functions in `threadSafeMap` panic when an index function returns an error.

If a disk-backed cache logs an error or returns partial data during a read, the reflector continues processing events while the local cache drifts silently out of sync with the Kubernetes apiserver. This desynchronization can lead to stale routing state, missing endpoints, or incorrect gateway configuration.

### Error Handling Policy

The adapter enforces a single fail-fast policy across all operations:
1. **Mandatory fatal handler:** Construction of `Indexer` requires an `OnFatal func(error)` callback. The provided `DefaultOnFatal` logs the error at error level and exits the process (`os.Exit(1)`).
2. **Unified trigger conditions:** `OnFatal` is invoked (via `sync.Once` to ensure execution exactly once) upon:
   - Any engine error returned from `Put`, `Delete`, `Get`, `Scan`, or `Sync`;
   - Asynchronous engine failure notifications delivered via `engine.Failed()`;
   - Object serialization or deserialization failures;
   - Decryption failures;
   - Index-function errors returned by registered secondary indexers.
3. **No partial results:** Methods without error returns (`List`, `ListKeys`) and methods with error returns (`ByIndex`) discard any accumulated items and return empty results if an error occurs mid-traversal. If the fatal handler does not terminate the process (as in unit test harnesses), subsequent operations continue to return errors or empty results.

### Trade-offs

- **Process restarts vs. silent divergence:** Halting the process when storage fails relies on the pod supervisor (kubelet) to restart the container. The restarted container wipes corrupted state (if marked as failed) and relists all objects from the apiserver. This incurs startup latency and apiserver load during relisting, but prevents controllers from operating on partial or inconsistent state.
- **Simplicity vs. in-flight retry:** The fail-fast model avoids complex in-memory staging queues, retry loops, or multi-version rollback mechanisms in the cache layer.

---

## Architecture and On-Disk Formats

### 1. Write-Ahead Log (WAL)

The write-ahead log records all state changes prior to page modification.

#### Segment File Lifecycle
- Log segments are named strictly by their starting 64-bit LSN, zero-padded to 20 decimal digits (e.g. `00000000000000000001.wal`).
- **Never reuse or recycle segment file names:** Every segment name is unique and monotonically ordered.
- After creating any new segment file, the parent directory is immediately fsynced (`fsync(dir)`).
- Finished segments are closed and become immutable artifacts suitable for remote archiving.
- Segments are truncated (deleted) only when fully covered by a completed checkpoint (i.e. strictly before the checkpoint's redo start LSN).

#### Segment Header (32 bytes)
Every WAL segment starts with a 32-byte header with 8 bytes of zero padding after the version:
```
+---------------+---------------+---------------+---------------------------------------+
|  Magic (4B)   |  Version (4B) | Reserved (8B) |           Database ID (16B)           |
|   "WAL1"      |      0x1      |  0x00 padding |     Random 128-bit store identifier   |
+---------------+---------------+---------------+---------------------------------------+
```

#### Record Format (36 bytes + dynamic payload)
Log records encode operations (`Put = 1`, `Delete = 2`, `Checkpoint = 3`). The record header is aligned on a 4-byte boundary, with explicit magic, format version, operation type, and reserved bytes preceding the 64-bit and 32-bit fields:
```
+-----------+-----------+-----------+---------------+-----------+-------------------+-------------+----------------+-------------+------------+
| Magic(1B) |Format(1B) | Type (1B) | Reserved (1B) |  LSN (8B) | RedoStartLSN (8B) | KeyLen (4B) | PayloadLen (4B)| RVLen (4B)  | CRC32C(4B) |
+-----------+-----------+-----------+---------------+-----------+-------------------+-------------+----------------+-------------+------------+
| Key bytes (KeyLen) ...                    | Payload bytes (PayloadLen) ...        | ResourceVersion bytes (RVLen) ...           |
+-------------------------------------------+---------------------------------------+---------------------------------------------+
```
- Monotonic 64-bit LSNs never reset during the lifetime of the store.
- CRC32C is computed with Castagnoli polynomial over the entire record with the CRC field zeroed.
- Checkpoint records specify the `RedoStartLSN` determining the point from which WAL redo must begin.

---

### 2. Heap File of Self-Describing 4 KiB Blocks

The heap file is organized into discrete 4096-byte blocks.

#### Block Header Layout (32 bytes)
**Every single 4 KiB block** begins with a compact 32-byte header layout:
```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   Magic (1B)  |  Version (1B) |          Reserved (2B)        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          CRC32C (4B)                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                   Logical Page Number (8B)                    |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                 Log Sequence Number - LSN (8B)                |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       Block Index (4B)        |        Num Blocks (4B)        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```
- Usable data area per block: `4096 - 32 = 4064 bytes`.
- The Database ID is not duplicated on individual page blocks (it resides in WAL segment headers and store metadata), saving 32 bytes per 4 KiB block.
- CRC32C is stored in the first 8-byte row and computed over all 4096 bytes with bytes 4–8 zeroed.

#### Slotted Page (1 Block = 4096 Bytes)
Small records share a 1-block slotted page:
- **Header:** Bytes 0–32: `BlockHeader`.
- **Metadata:** Bytes 32–40: `NumSlots (2B)`, `FreeStart (2B)`, `FreeEnd (2B)`, `Flags (2B)`.
- **Slot Directory:** Bytes 40 onwards: array of slots `{ Offset uint16, Length uint16 }`.
- **Stable Slot IDs:** When records are deleted, their slot entry is marked as a tombstone (`Offset = 0, Length = 0`). The slot index remains reserved until reused by a new record.
- **Defragmentation / Compaction:** Active payloads grow downwards from byte 4096. Compaction slides all active payloads to the end of the block and updates slot offsets, maintaining original slot indices without renumbering.
- **Empty Pages:** Emptied pages are written with `NumSlots = 0`. Logical page IDs are never discarded; empty logical pages are recycled first.

#### Multi-Block Page (Power-of-Two Blocks: 2 to 512 Blocks = up to 2 MiB)
Large objects span power-of-two contiguous physical blocks:
- **Every single block** in the run contains the 32-byte `BlockHeader` with `BlockIndex = i`, `NumBlocks = N`, and the page's `LogicalPage` and `LSN`.
- Block 0 stores the record header: `KeyLen (2B)`, `PayloadLen (4B)`, `LSN (8B)`, `Key`.
- Payload bytes stream across the usable payload section (`block[32:4096]`) of blocks `0` through `N-1`.
- Objects larger than 2 MiB are classified as oversize, kept in memory, counted in `OversizeObjectsCount`, and written without failing.

---

### 3. Value Compression and Page Class Selection

To conserve disk space and reduce multi-block power-of-two page allocation overhead, the engine compresses values that exceed the single-block slotted page payload threshold:

- **Compression Scope:** Only object values are compressed. Object keys, WAL record headers, and physical 4 KiB block headers remain uncompressed.
- **Supported Codecs:**
  - `CodecNone (0x00)`: Value stored raw without compression.
  - `CodecZstd (0x01)`: Value compressed using pure Go `zstd` at its fastest level (`zstd.SpeedFastest`). zstd is the default codec.
  - `CodecS2 (0x02)`: Value compressed using pure Go `s2` block format.
  - `CodecSnappy (0x03)`: Value compressed using pure Go `snappy` block format (`github.com/klauspost/compress/snappy`).
- **On-Disk Value Record Format:**
  - Stored raw (`CodecNone`): `[Codec (1B)][Raw Payload Bytes...]`
  - Stored compressed (`CodecZstd`, `CodecS2`, or `CodecSnappy`): `[Codec (1B)][Uncompressed Length uint32 Big-Endian (4B)][Compressed Stream Bytes...]`
- **Configurable Threshold:** Defaults to `DefaultCompressionThreshold = BlockUsableSize - 64` (4000 bytes, the 1-block slotted page capacity). Objects smaller than or equal to the threshold are stored raw. Callers configure this threshold via `WithCompressionThreshold(bytes)`.
- **10% Savings Rule:** If an object exceeds the threshold but its compressed size is not at least about 10% smaller than the raw payload (`compressedLen * 10 <= rawLen * 9`), the value is stored raw with `CodecNone` to avoid compression overhead for incompressible data.
- **Page Class Selection from Compressed Size:** The page class (1-block slotted page vs. power-of-two multi-block page) is selected using the compressed payload length. An object that is 10 KB uncompressed drops from a 4-block page (16 KiB) to a 1-block slotted page (4 KiB) after compression.
- **Compress Once, Write Same Bytes Twice:** Both the WAL record and heap page store the compressed (and optionally encrypted) payload. This cuts log segment volume in half for large objects and avoids decompressing during WAL redo replay.
- **Ordering with Encryption:** Compression runs before authenticated symmetric encryption (`compress -> encrypt -> toStore`). Ciphertext does not compress; decrypting before decompressing ensures confidentiality.
- **Bound Decompression & Corruption Resistance:** Every compressed record records its 32-bit uncompressed length. The engine rejects any record whose recorded length exceeds `MaxObjectSize` (default 64 MiB) prior to memory allocation or decompressor invocation. After decompression, the engine verifies that the output length matches the recorded length. Length mismatches, truncated byte streams, and unknown codec bytes are treated as corruption, transitioning the engine into the permanent failed state without panicking or allocating large buffers.
- **Pooled Encoders and Decoders:** zstd encoders and decoders are expensive to allocate and are reused via `sync.Pool`. Decoders configure `zstd.WithDecodeAllCapLimit(true)` to enforce bounded memory allocation even on malformed inputs.

---

### 4. Out-Of-Place Writes and Buddy Allocation

#### Storage Invariants
1. **Reuse Rule:** The disk location holding a page's newest *durable* (fsynced) copy must never be overwritten. Older copies become free for reuse only after a newer copy has been fsynced to disk. Blocks written since the last fsync are tracked in memory and may be freed immediately if superseded.
2. **Write-Ahead Rule:** Before writing any page with LSN `L` to the heap file, the WAL must be fsynced through `L`.

#### Buddy Allocator & Tail File Truncation
- Manages block ranges in powers-of-two (order 0 = 1 block, order 9 = 512 blocks).
- Always allocates the **lowest available free location** across all eligible orders, packing active pages towards the beginning of the file.
- When the tail of the heap file empties, `TruncateTail()` invokes `ftruncate()` to shrink the physical file.

---

### 5. Buffer Pool and Direct I/O Engine

- **Fixed-Size Arena:** A single contiguous `[]byte` slice aligned to 4096 bytes without using `mmap`. Because Go slices of primitive types contain no pointers, the Go runtime garbage collector skips scanning the arena.
- **Direct I/O:**
  - On Linux: opened with `O_DIRECT`. Reads and writes use `unix.Preadv` and `unix.Pwritev` via `SyscallConn.Control()`, ensuring zero kernel page-cache copying.
  - On macOS: opened with `F_NOCACHE` via `unix.FcntlInt` for test execution.
- **Non-Adjacent Frames:** Multi-block pages in memory can occupy non-adjacent buffer frames. `DirectIO.Writev` and `DirectIO.Readv` transfer all frames in a single vectored system call.
- **Background Cleaner:** A background goroutine flushes unpinned dirty pages under the write-ahead rule to keep clean frames available for foreground allocations.

---

## Crash Consistency and Recovery Algorithm

```
                           +--------------------------+
                           |  Crash Recovery Trigger  |
                           +--------------------------+
                                        |
                                        v
                           +--------------------------+
                           |  Physical 4 KiB Block    |
                           |  Scan of Heap File       |
                           +--------------------------+
                                        |
                  +---------------------+---------------------+
                  |                                           |
                  v                                           v
       [Valid Header & CRC32C]                 [Invalid Magic/Ver or Corrupt]
                  |                                           |
                  v                                           v
       +-----------------------+                         (Discarded)
       | Group Blocks into     |
       | Candidate Page Runs   |
       +-----------------------+
                  |
        +---------+---------+
        |                   |
        v                   v
   [Complete Page]      [Torn / Missing Block]
        |                   |
        v                   v
   (Keep Candidate)     (Ignore Candidate, Fallback)
        |
        v
   +---------------------------------------+
   | For each Logical Page:                |
   | Select Candidate with Highest LSN     |
   +---------------------------------------+
        |
        v
   +---------------------------------------+
   | Reconstruct Buddy Allocator Bitmap    |
   | and In-Memory Key Index               |
   +---------------------------------------+
        |
        v
   +---------------------------------------+
   | Scan WAL for Last Checkpoint Record   |
   | (Determine RedoStartLSN)              |
   +---------------------------------------+
        |
        v
   +---------------------------------------+
   | Replay WAL from RedoStartLSN          |
   | Apply only if Record LSN > Page LSN   |
   +---------------------------------------+
        |
        v
   +---------------------------------------+
   | Report Last Recovered ResourceVersion |
   +---------------------------------------+
```

### Recovery Steps
1. **Physical Block Scan:** Reads the heap file in sequential 256 KiB chunks. For every 4 KiB block, validates `BlockMagic` (0x47), format version (1), and Castagnoli CRC32C.
2. **Candidate Filtering:** Validates that candidate pages form complete buddy runs of `NumBlocks` with matching `LogicalPage`, `LSN`, and sequential `BlockIndex` from `0` to `NumBlocks - 1`. If any block in a run is missing, torn, or invalid, the candidate is discarded, and recovery falls back to the previous durable copy.
3. **Winner Selection:** For each logical page number, the candidate copy with the maximum LSN is chosen.
4. **Index Rebuilding:**
   - Slotted pages are inspected: active slots populate the key index `(logicalPage, slot, LSN)`, always selecting the highest LSN across pages, and empty pages (`NumSlots = 0`) are registered in `emptyLogicalPages`.
   - Multi-block pages populate slot 0.
   - Allocated blocks are marked in the buddy allocator.
5. **WAL Redo Replay:**
   - Identifies the last checkpoint record to find `RedoStartLSN`.
   - Iterates records from `RedoStartLSN` to EOF. Replay stops at the first record with a corrupted CRC or non-contiguous LSN, or advances across segments if a newer segment starts at the expected LSN.
   - For each record, if `record.LSN > page.LSN`, the modification is applied directly to the page buffer and persisted to disk.
6. **Watch Resumption:** The highest recovered `ResourceVersion` is reported so reflectors can resume watches without re-listing.

---

## Threat Model: Forged Block Headers

### The Threat
An informer object contains untrusted user data. A malicious user could submit a Kubernetes ConfigMap or Secret whose data payload contains bytes crafted to look like a valid block header:
- Valid `BlockMagic` (`0x47`) and version `1`
- A forged target `LogicalPage`
- An artificially high `LSN` (e.g. `999999`)
- A valid Castagnoli CRC32C checksum

If a recovery scanner were tricked into accepting this payload as a valid block header, the malicious object could overwrite or resurrect arbitrary database pages after a crash.

### Architectural Guarantee
Recovery never trusts bytes that object payloads control:
1. **Strict 4 KiB Block Boundary Alignment:** The recovery engine only parses block headers at exact 4096-byte boundaries (`offset % 4096 == 0`).
2. **Per-Block Header Isolation:** In `DiskStore`, **every 4096-byte block on disk** starts with the store's 32-byte `BlockHeader` written by the storage engine, validated with a Castagnoli CRC32C checksum.
3. **Payload Containment:**
   - In slotted pages: payload bytes are stored inside slot records within `block[40:4096]`.
   - In multi-block pages: payload bytes are partitioned across `block[32:4096]`.
   - **Payload bytes are never written at byte offset 0 of any 4 KiB block on disk.**
4. **Empirical Verification:** The test `TestRecovery_ForgedHeaders` constructs multi-block payloads containing valid forged block headers with high LSNs placed at every offset. Upon crash recovery, the store correctly recognizes genuine headers at block boundaries, verifies the genuine payload, and rejects any forged header.

---

## Comparison with BBolt and In-Memory Informer

### Detailed Benchmark Results Across Realistic Workloads

*Measured on Linux 6.6 amd64 with direct I/O enabled and an 8 MiB (2,048-frame) buffer pool.*

#### 1. Configuration Text (JSON) (2,500 objects, ~10 KB each)

```
================================================================================================================================================
COMPARATIVE BENCHMARK: Configuration Text (JSON) (2500 objects, ~10 KB each)
================================================================================================================================================
Storage Engine                 | Heap In-Use  | Process RSS  | Disk Size    | Write Time   | Write Thruput  | Recovery    
------------------------------------------------------------------------------------------------------------------------------------------------
In-Memory Informer             |      28.83 MB |      45.77 MB |   0 MB (RAM) |       0.00 s |    2339028 op/s |            -
BBolt (mmap + NoSync)          |      28.80 MB |      82.41 MB |     48.09 MB |       0.20 s |      12699 op/s |     12.00 ms
DiskStore (None)               |      28.66 MB |      79.41 MB |     47.95 MB |       0.69 s |       3631 op/s |    162.00 ms
DiskStore (zstd)               |      28.69 MB |      76.14 MB |      8.51 MB |       0.23 s |      11056 op/s |     44.00 ms
DiskStore (S2)                 |      28.69 MB |      82.27 MB |     16.23 MB |       0.43 s |       5798 op/s |    139.00 ms
DiskStore (Snappy)             |      28.70 MB |      77.47 MB |     15.62 MB |       0.40 s |       6196 op/s |     71.00 ms
------------------------------------------------------------------------------------------------------------------------------------------------
Recompute Latency Comparison:
  In-Memory Informer     :         0 refs =   0.00 µs |   10 refs =  12.00 µs | 1000 refs =   416.00 µs
  BBolt (mmap + NoSync)   (Cold): 0 refs =   5.00 µs |   10 refs = 181.00 µs | 1000 refs = 20842.00 µs
  BBolt (mmap + NoSync)   (Warm): 0 refs =   0.00 µs |   10 refs =   9.00 µs | 1000 refs =  1149.00 µs
  DiskStore (None)        (Cold): 0 refs =   0.00 µs |   10 refs = 3644.00 µs | 1000 refs = 267040.00 µs
  DiskStore (None)        (Warm): 0 refs =   0.00 µs |   10 refs =  93.00 µs | 1000 refs = 264492.00 µs
  DiskStore (zstd)        (Cold): 0 refs =   0.00 µs |   10 refs = 206.00 µs | 1000 refs = 15318.00 µs
  DiskStore (zstd)        (Warm): 0 refs =   0.00 µs |   10 refs = 130.00 µs | 1000 refs = 19964.00 µs
  DiskStore (S2)          (Cold): 0 refs =   0.00 µs |   10 refs = 1803.00 µs | 1000 refs = 180449.00 µs
  DiskStore (S2)          (Warm): 0 refs =   0.00 µs |   10 refs =  78.00 µs | 1000 refs =  9933.00 µs
  DiskStore (Snappy)      (Cold): 0 refs =   0.00 µs |   10 refs = 1793.00 µs | 1000 refs = 162689.00 µs
  DiskStore (Snappy)      (Warm): 0 refs =   0.00 µs |   10 refs =  65.00 µs | 1000 refs =  9616.00 µs
================================================================================================================================================
```

#### 2. TLS Material (x509 PEM) (2,500 objects, ~10 KB each)

```
================================================================================================================================================
COMPARATIVE BENCHMARK: TLS Material (x509 PEM) (2500 objects, ~10 KB each)
================================================================================================================================================
Storage Engine                 | Heap In-Use  | Process RSS  | Disk Size    | Write Time   | Write Thruput  | Recovery    
------------------------------------------------------------------------------------------------------------------------------------------------
In-Memory Informer             |      30.24 MB |      -2.69 MB |   0 MB (RAM) |       0.00 s |    1906796 op/s |            -
BBolt (mmap + NoSync)          |      30.13 MB |      44.77 MB |     48.09 MB |       0.19 s |      13278 op/s |     12.00 ms
DiskStore (None)               |      29.96 MB |      30.23 MB |     48.01 MB |       0.64 s |       3882 op/s |    156.00 ms
DiskStore (zstd)               |      29.96 MB |      31.79 MB |     16.20 MB |       0.43 s |       5755 op/s |     62.00 ms
DiskStore (S2)                 |      29.96 MB |      30.46 MB |     17.85 MB |       0.48 s |       5256 op/s |     63.00 ms
DiskStore (Snappy)             |      29.97 MB |      42.64 MB |     18.39 MB |       0.47 s |       5358 op/s |     73.00 ms
------------------------------------------------------------------------------------------------------------------------------------------------
Recompute Latency Comparison:
  In-Memory Informer     :         0 refs =   0.00 µs |   10 refs =  11.00 µs | 1000 refs =   196.00 µs
  BBolt (mmap + NoSync)   (Cold): 0 refs =   5.00 µs |   10 refs =  49.00 µs | 1000 refs =  1218.00 µs
  BBolt (mmap + NoSync)   (Warm): 0 refs =   0.00 µs |   10 refs =  22.00 µs | 1000 refs =   702.00 µs
  DiskStore (None)        (Cold): 0 refs =   0.00 µs |   10 refs =  12.00 µs | 1000 refs =   283.00 µs
  DiskStore (None)        (Warm): 0 refs =   0.00 µs |   10 refs =   2.00 µs | 1000 refs =   223.00 µs
  DiskStore (zstd)        (Cold): 0 refs =   0.00 µs |   10 refs =  12.00 µs | 1000 refs =   273.00 µs
  DiskStore (zstd)        (Warm): 0 refs =   0.00 µs |   10 refs =   2.00 µs | 1000 refs =   240.00 µs
  DiskStore (S2)          (Cold): 0 refs =   0.00 µs |   10 refs =  52.00 µs | 1000 refs =   434.00 µs
  DiskStore (S2)          (Warm): 0 refs =   0.00 µs |   10 refs =   3.00 µs | 1000 refs =   384.00 µs
  DiskStore (Snappy)      (Cold): 0 refs =   0.00 µs |   10 refs =  11.00 µs | 1000 refs =   278.00 µs
  DiskStore (Snappy)      (Warm): 0 refs =   0.00 µs |   10 refs =   2.00 µs | 1000 refs =   236.00 µs
================================================================================================================================================
```

#### 3. Random Incompressible Bytes (2,500 objects, ~10 KB each)

```
================================================================================================================================================
COMPARATIVE BENCHMARK: Random Incompressible Bytes (2500 objects, ~10 KB each)
================================================================================================================================================
Storage Engine                 | Heap In-Use  | Process RSS  | Disk Size    | Write Time   | Write Thruput  | Recovery    
------------------------------------------------------------------------------------------------------------------------------------------------
In-Memory Informer             |      27.69 MB |     -20.23 MB |   0 MB (RAM) |       0.00 s |    2389144 op/s |            -
BBolt (mmap + NoSync)          |      27.60 MB |      41.75 MB |     48.07 MB |       0.19 s |      13390 op/s |     12.00 ms
DiskStore (None)               |      27.38 MB |      25.95 MB |     47.96 MB |       0.69 s |       3622 op/s |    158.00 ms
DiskStore (zstd)               |      27.38 MB |      27.96 MB |     47.96 MB |       0.64 s |       3889 op/s |    147.00 ms
DiskStore (S2)                 |      27.38 MB |      22.90 MB |     47.96 MB |       0.65 s |       3836 op/s |    148.00 ms
DiskStore (Snappy)             |      27.40 MB |      25.26 MB |     47.96 MB |       0.64 s |       3899 op/s |    152.00 ms
------------------------------------------------------------------------------------------------------------------------------------------------
Recompute Latency Comparison:
  In-Memory Informer     :         0 refs =   0.00 µs |   10 refs =  18.00 µs | 1000 refs =   344.00 µs
  BBolt (mmap + NoSync)   (Cold): 0 refs =   3.00 µs |   10 refs =  36.00 µs | 1000 refs =  1090.00 µs
  BBolt (mmap + NoSync)   (Warm): 0 refs =   1.00 µs |   10 refs =  13.00 µs | 1000 refs =   841.00 µs
  DiskStore (None)        (Cold): 0 refs =   0.00 µs |   10 refs =  14.00 µs | 1000 refs =   279.00 µs
  DiskStore (None)        (Warm): 0 refs =   0.00 µs |   10 refs =   2.00 µs | 1000 refs =   226.00 µs
  DiskStore (zstd)        (Cold): 0 refs =   0.00 µs |   10 refs =  15.00 µs | 1000 refs =   351.00 µs
  DiskStore (zstd)        (Warm): 0 refs =   0.00 µs |   10 refs =   2.00 µs | 1000 refs =   215.00 µs
  DiskStore (S2)          (Cold): 0 refs =   0.00 µs |   10 refs =  14.00 µs | 1000 refs =   286.00 µs
  DiskStore (S2)          (Warm): 0 refs =   0.00 µs |   10 refs =   2.00 µs | 1000 refs =   217.00 µs
  DiskStore (Snappy)      (Cold): 0 refs =   0.00 µs |   10 refs =  20.00 µs | 1000 refs =   293.00 µs
  DiskStore (Snappy)      (Warm): 0 refs =   0.00 µs |   10 refs =   2.00 µs | 1000 refs =   258.00 µs
================================================================================================================================================
```

#### 4. Mixed Realistic Corpus (10,000 objects, ~100 MB data)

```
================================================================================================================================================
COMPARATIVE BENCHMARK: Mixed Realistic Corpus (10000 objects, ~10 KB each)
================================================================================================================================================
Storage Engine                 | Heap In-Use  | Process RSS  | Disk Size    | Write Time   | Write Thruput  | Recovery    
------------------------------------------------------------------------------------------------------------------------------------------------
In-Memory Informer             |     115.94 MB |     103.48 MB |   0 MB (RAM) |       0.01 s |    1831254 op/s |            -
BBolt (mmap + NoSync)          |     115.33 MB |     301.85 MB |    144.25 MB |       0.73 s |      13767 op/s |     12.00 ms
DiskStore (None)               |     114.76 MB |     187.31 MB |    159.90 MB |       3.87 s |       2586 op/s |    555.00 ms
DiskStore (zstd)               |     114.77 MB |     197.62 MB |     92.71 MB |       3.30 s |       3026 op/s |    350.00 ms
DiskStore (S2)                 |     114.77 MB |     183.43 MB |     82.73 MB |       3.08 s |       3244 op/s |    296.00 ms
DiskStore (Snappy)             |     114.78 MB |     181.45 MB |     82.61 MB |       3.13 s |       3194 op/s |    310.00 ms
------------------------------------------------------------------------------------------------------------------------------------------------
Recompute Latency Comparison:
  In-Memory Informer     :         0 refs =   0.00 µs |   10 refs =  11.00 µs | 1000 refs =   338.00 µs
  BBolt (mmap + NoSync)   (Cold): 0 refs =   5.00 µs |   10 refs =  85.00 µs | 1000 refs =  3869.00 µs
  BBolt (mmap + NoSync)   (Warm): 0 refs =   1.00 µs |   10 refs =   9.00 µs | 1000 refs =   714.00 µs
  DiskStore (None)        (Cold): 0 refs =   0.00 µs |   10 refs = 1515.00 µs | 1000 refs = 120400.00 µs
  DiskStore (None)        (Warm): 0 refs =   0.00 µs |   10 refs =  36.00 µs | 1000 refs =  3353.00 µs
  DiskStore (zstd)        (Cold): 0 refs =   0.00 µs |   10 refs = 1188.00 µs | 1000 refs = 77373.00 µs
  DiskStore (zstd)        (Warm): 0 refs =   0.00 µs |   10 refs =  62.00 µs | 1000 refs =  5088.00 µs
  DiskStore (S2)          (Cold): 0 refs =   0.00 µs |   10 refs = 1148.00 µs | 1000 refs = 80150.00 µs
  DiskStore (S2)          (Warm): 0 refs =   0.00 µs |   10 refs =  48.00 µs | 1000 refs =  2774.00 µs
  DiskStore (Snappy)      (Cold): 0 refs =   0.00 µs |   10 refs = 1263.00 µs | 1000 refs = 70304.00 µs
  DiskStore (Snappy)      (Warm): 0 refs =   0.00 µs |   10 refs =  27.00 µs | 1000 refs =  2659.00 µs
================================================================================================================================================
```

### Analysis of Trade-offs

1. **Why Compression Accelerates Write Throughput:**
   - Uncompressed storage writes 4 physical blocks for every 10 KB object. For 10,000 objects, that totals 40,000 blocks written through an 8 MiB (2,048-frame) buffer pool. The buffer pool experiences continuous frame eviction and dirty page writeback.
   - The engine issues one write call per WAL block, and one write call per page during flushes.
   - When values compress, objects fit within single-block slotted pages or smaller multi-block allocations (e.g. 1 or 2 blocks). The reduced working set fits within the buffer pool without continuous eviction churn. Fewer pages and fewer blocks reduce the number of write system calls, which accelerates write throughput.
   - Batched writes would raise the uncompressed baseline; that architectural enhancement is tracked separately.

2. **Heap Usage and Goroutine Lifecycle:**
   - Heap in-use remains flat and consistent across all storage engines and compression codecs (~28.7 MB for 2,500 objects; ~114.8 MB for 10,000 objects).
   - Closing the engine signals its `Closed()` channel, which terminates the adapter's failure-watch goroutine. This releases references to the engine and its buffer-pool arena, preventing memory retention across engine lifecycles.

3. **Codec Trade-offs (zstd vs. S2 vs. Snappy):**
   - **Configuration Text (JSON):** `zstd` produces the smallest disk footprint (**8.51 MB**, an 82.3% reduction), outperforming S2 (**16.23 MB**) and Snappy (**15.62 MB**) by almost a factor of two. Because the data compresses into fewer pages, `zstd` achieves the highest write throughput (**11,056 op/s** vs. **5,798 op/s** for S2 and **6,196 op/s** for Snappy) and fastest recovery (**44 ms** vs. **139 ms** / **71 ms**).
   - **TLS Material (x509 PEM):** `zstd` achieves the highest compression ratio (**16.20 MB**, a 66.3% reduction) compared to S2 (**17.85 MB**) and Snappy (**18.39 MB**).
   - **Incompressible Random Bytes:** All three codecs detect that compressed output fails the 10% savings threshold and fall back to raw storage (`CodecNone`), consuming identical disk space (**47.96 MB**) with identical write throughput (~3,800 op/s).
   - **Verdict:** `zstd` is selected as the default codec because it delivers the highest compression ratio and highest throughput on compressible Kubernetes configuration data.

4. **Process RSS & Container Memory Limits:**
   - `BBolt` resident set size balloons to **301.85 MB** on the 10,000 mixed object workload because operating system page caching retains mmap pages in the container cgroup.
   - `DiskStore` caps Process RSS at **181–197 MB** across all codecs by using direct I/O and a fixed 8 MiB buffer pool arena, avoiding kernel page-cache amplification.

5. **Crash Recovery Time:**
   - On compressible configuration text, `DiskStore (zstd)` recovers in **44 ms**, compared to **162 ms** uncompressed. On the 10,000 mixed corpus, recovery completes in **350 ms**, compared to **555 ms** uncompressed. Replaying WAL redo records copies compressed bytes directly into pages without decompression, minimizing recovery latency.

6. **Broad Read Isolation and Locking Trade-offs (`List` vs Concurrent Writers):**
   - **Point-in-Time Consistency:** `DiskStore.List(bucketName)` acquires `s.writerMu.RLock()` for the duration of the broad bucket read and object decoding. This provides point-in-time consistency across the bucket: concurrent writes (`Put`) or deletions (`Delete`) cannot mutate page contents, renumber slots during compaction, or modify the key index while objects are being retrieved and decoded.
   - **Scope and Trade-off Analysis:** Because `writerMu` is store-wide, holding `writerMu.RLock()` stalls writes to every bucket for the duration of a `List` while all objects in the bucket are iterated and decoded into memory. An alternative architecture would provide non-blocking concurrency via per-page versioning or multi-version concurrency control (MVCC); however, MVCC introduces additional memory overhead, garbage collection complexity, and page table churn. The trade-off is that writes stall for the duration of a `List`, in exchange for a consistent view without per-page versioning.
   - **Alignment with Informer Access Patterns:** GARI consumes these caches through informer event handlers and doesn't call `List` on them, so broad reads mostly happen at startup and relist. Other controllers may differ in their access patterns, so workloads that frequently invoke `List` concurrently with active write streams would experience write stalls under this locking model.

---

## Conclusion

The `DiskStore` prototype proves that a write-ahead log combined with a self-describing 4 KiB block heap file, value compression, and fixed-size direct I/O buffer pool successfully resolves the `mmap` and durability flaws of `bbolt`. It provides:
- **82.3% lower disk usage** on configuration text (JSON) and **66.3% lower disk usage** on TLS material with zstd value compression.
- **34.5% lower memory footprint (RSS)** than bbolt on mixed realistic workloads.
- **3x higher write throughput** on configuration text through reduced buffer pool eviction churn.
- **Strict crash consistency** with out-of-place writes and the Reuse Rule.
- **Resilience against untrusted payload forgery and corrupt decompression inputs**.
- **Full `cache.Indexer` conformance**.
