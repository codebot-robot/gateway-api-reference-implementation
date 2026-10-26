# Design: Disk-Store Engine

This document describes the architecture, terminology, on-disk formats, storage invariants, recovery process, and concurrency model of the disk-store engine (`experiments/diskstore/engine`).
For benchmark results and performance measurements, see [diskstore-experiment.md](../../../docs/diskstore-experiment.md).

## Terminology

The engine uses the following terms:

- **Block.** The 4 KiB unit, sized to match the disk. A block lives on disk, in the heap file at a block number, and may also be resident in memory in the block pool. A heap block starts with a 32-byte header carrying the logical page number, LSN, its index within the page, the page's block count, and a CRC32C checksum.
- **Block pool.** The fixed-size, pointer-free in-memory arena of blocks that the code currently calls the buffer pool (`BufferPool`). A resident page occupies one pool block per page block, and those pool blocks do not need to be adjacent.
- **Page.** The logical unit, identified by a logical page number. It is either a slotted page (one block holding many small records, each in a record slot) or a multi-block page ($2^k$ blocks holding one large record). The engine writes pages out of place, so a logical page moves between block numbers over time.
- **Block run.** A contiguous, aligned run of blocks allocated by the buddy allocator. A multi-block page always occupies one block run, and a flush batch writes several pages into one block run.
- **WAL terms:**
  - **Segment.** An append-only log file named by its starting 64-bit LSN, formatted as a 20-digit zero-padded decimal number with a `.wal` extension. The engine appends records to the active segment. In accordance with invariant 4, once a 4 KiB block within a segment is fsynced to disk, the engine never rewrites that block.
  - **Record.** A single log entry recording an operation, such as a write, deletion, checkpoint, or progress token.
  - **LSN.** A 64-bit monotonically increasing Log Sequence Number.
  - **Durable LSN.** The highest LSN confirmed flushed and fsynced to disk.

### Identifier Mapping

The code contains legacy identifiers that will be renamed in separate work. The following table maps design terms to current code identifiers:

| Design Term | Current Code Identifier | Description in Code |
| :--- | :--- | :--- |
| Block | `BlockSize`, `BlockHeader`, `PhysicalBlock`, `FlushedLoc`, `DurableLoc` | The 4 KiB unit on disk and in memory. |
| Block pool | `BufferPool`, `NewBufferPool` | The in-memory pool managing resident memory blocks. |
| Pool block | `FrameIDs`, `freeFrames`, `numFrames`, `NumFrames`, `DefaultBufferPoolFrames` | An in-memory 4 KiB block in the pool arena (currently named "frame"). |
| Page | `PageDesc`, `PageHandle`, `LogicalPage`, `SlottedPage`, `MultiBlockPage` | The logical unit identified by a logical page number. |
| Block run | `AllocateBatch`, `MaxBatchBlocks`, buddy allocations | An aligned, contiguous run of physical blocks allocated together (currently called "extent" or "batch"). |
| Record slot | `SlottedPage` slot entries (`Offset`, `Length`), `Slot`, `KeyIndexEntry.Slot` | An entry in the slot directory of a slotted page pointing to a record payload. |
| Segment | `segmentFileName`, `WAL.activeFile`, `*.wal` files | A WAL segment file named after its starting LSN. |
| Record | `Record`, `EncodeRecord`, `DecodeRecord` | A WAL entry for an operation. |
| LSN | `LSN`, `nextLSN`, `lastAppendedLSN`, `PageLSN`, `DirtyLSN` | The 64-bit monotonic sequence number. |
| Durable LSN | `WAL.DurableLSN()`, `WAL.fsyncedLSN` | The maximum LSN confirmed written and fsynced to disk. |

## On-Disk Formats

The engine defines binary layouts for heap blocks, slotted pages, multi-block pages, WAL segment headers, and WAL records. All multi-byte integers use big-endian encoding.

### Block Header

Every 4 KiB heap block begins with a 32-byte header. The header makes each block self-describing on disk:

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

The header fields contain the following values:
- **Magic (1 byte):** The constant `0x47` (ASCII `'G'`).
- **Version (1 byte):** The constant `1`.
- **Reserved (2 bytes):** Zero padding for 4-byte boundary alignment.
- **CRC32C (4 bytes):** Castagnoli CRC32C checksum computed over the full 4096-byte block with bytes 4 through 7 zeroed.
- **Logical Page Number (8 bytes):** The logical page identifier for this block.
- **LSN (8 bytes):** The log sequence number of the most recent modification to this page.
- **Block Index (4 bytes):** The index of this block within the page, ranging from 0 to `NumBlocks - 1`.
- **Num Blocks (4 bytes):** The total count of blocks in the page. This value is a power of two from 1 to 512.

Each block provides 4064 bytes of usable data space (`BlockSize - BlockHeaderSize`).

### Slotted Page

A slotted page occupies a single 4 KiB block. It stores multiple small records, each assigned to a record slot.

The slotted page layout divides the 4096 bytes into four sections:
1. **Block Header (bytes 0 to 31):** The standard 32-byte block header.
2. **Page Metadata (bytes 32 to 39):**
   - `NumSlots` (2 bytes): Total number of record slots allocated in the directory.
   - `FreeStart` (2 bytes): Byte offset where free space for new record slots starts. It begins at byte 40.
   - `FreeEnd` (2 bytes): Byte offset where free space for new payloads ends. It begins at byte 4096.
   - `Flags` (2 bytes): Reserved flags.
3. **Slot Directory (byte 40 to FreeStart):** An array of 4-byte record slot entries. Each entry contains:
   - `Offset` (2 bytes): Byte offset of the record payload.
   - `Length` (2 bytes): Length of the record payload.
   The slot directory grows upward toward `FreeEnd`.
4. **Record Payloads (FreeEnd to byte 4096):** Record payloads grow downward from byte 4096 toward `FreeStart`.

Each record payload contains:
- `KeyLen` (2 bytes): Length of the key string.
- `PayloadLen` (4 bytes): Length of the value payload.
- `LSN` (8 bytes): Modification LSN for this record.
- `Key`: Raw bytes of the key string.
- `Payload`: Raw or compressed value bytes.

When a record is deleted, the engine sets `Offset` and `Length` to zero in that record slot. This tombstone keeps record slot numbers stable. The engine reuses tombstone record slots for subsequent inserts.

When active payloads fragment space, the engine compacts the page. Compaction slides all active payloads down toward byte 4096 and updates slot offsets in place. Record slot indices remain unchanged during compaction. When all records in a slotted page are deleted, `NumSlots` remains in place, but active record slots drop to zero. The engine then recycles the page as an empty logical page.

### Multi-Block Page

A multi-block page stores a single large record across a power-of-two block run ($2^k$ blocks, from 1 to 512 blocks, up to 2 MiB, as produced by `CalculateMultiBlockCount`).

The format distributes the record across the block run:
- Every block in the run begins with the 32-byte block header. Each block header carries the page's logical page number, LSN, total block count ($2^k$), and individual block index ($0$ through $2^k - 1$).
- The record header, key, and payload are written as one continuous stream starting at byte 32 of block 0 and continuing through the usable section (bytes 32 to 4095) of each successive block in the run:
  - `KeyLen` (2 bytes): Length of the key string.
  - `PayloadLen` (4 bytes): Length of the value payload.
  - `LSN` (8 bytes): Modification LSN.
  - `Key`: Raw bytes of the key string.
  - `Payload`: Raw or compressed value bytes.
  Because the engine writes this data as a single continuous stream, both the key and the payload can cross block boundaries.
- The in-memory key index maps the record to its logical page number and record slot 0.

### WAL Segment Header

Every WAL segment file begins with a 32-byte header:

```
+---------------+---------------+---------------+---------------------------------------+
|  Magic (4B)   |  Version (4B) | Reserved (8B) |           Database ID (16B)           |
|   "WAL1"      |      0x1      |  0x00 padding |     Random 128-bit store identifier   |
+---------------+---------------+---------------+---------------------------------------+
```

The header contains:
- **Magic (4 bytes):** The constant `0x57414C31` (ASCII `"WAL1"`).
- **Version (4 bytes):** The integer `1`.
- **Reserved (8 bytes):** Zero padding for alignment.
- **Database ID (16 bytes):** The unique 128-bit identifier of the store.

### WAL Record

Each WAL record contains a 36-byte header followed by variable-length fields:

```
+-----------+-----------+-----------+---------------+-----------+-------------------+-------------+----------------+-------------+------------+
| Magic(1B) |Format(1B) | Type (1B) | Reserved (1B) |  LSN (8B) | RedoStartLSN (8B) | KeyLen (4B) | PayloadLen (4B)| TokenLen(4B)| CRC32C(4B) |
+-----------+-----------+-----------+---------------+-----------+-------------------+-------------+----------------+-------------+------------+
| Key bytes (KeyLen) ...                    | Payload bytes (PayloadLen) ...        | Token bytes (TokenLen) ...                  |
+-------------------------------------------+---------------------------------------+---------------------------------------------+
```

The record fields contain:
- **Magic (1 byte):** The constant `0x52` (ASCII `'R'`).
- **Format Version (1 byte):** The integer `1`.
- **Operation Type (1 byte):** The operation code:
  - `1`: `Put`
  - `2`: `Delete`
  - `3`: `Checkpoint`
  - `4`: `Token`
- **Reserved (1 byte):** Zero padding.
- **LSN (8 bytes):** The monotonically assigned log sequence number.
- **RedoStartLSN (8 bytes):** The starting LSN for WAL redo replay. This field is populated on Checkpoint records.
- **KeyLen (4 bytes):** Length of the key in bytes.
- **PayloadLen (4 bytes):** Length of the payload in bytes.
- **TokenLen (4 bytes):** Length of the progress token string in bytes.
- **CRC32C (4 bytes):** Castagnoli CRC32C checksum computed over the record header and body with the CRC field zeroed.
- **Body:** The key bytes, payload bytes, and token bytes.

## Invariants

The engine maintains the following invariants:

### 1. The Write-Ahead Rule

Before the engine writes any page with LSN $L$ to the heap file, the WAL must be fsynced through at least LSN $L$.

This rule prevents disk inconsistency across crashes. If a host loses power after the heap file receives a page write, the WAL on disk will contain all log records up to that page's state. Crash recovery will never encounter heap data that outruns durable log history.

The block pool enforces this rule before flushing dirty pages. The flusher calculates the maximum LSN among all pages in the flush batch. It calls `wal.SyncThrough(maxLSN)` to flush and fsync the log before writing the block run to the heap file.

### 2. The Reuse Rule and Page Location Tracking

The disk location holding a page's newest durable (fsynced) copy must never be overwritten. Older locations become free for reuse only after a newer copy is confirmed durable by a heap file fsync.

The engine tracks page locations on disk using four descriptor fields in `PageDesc`:
- `PhysicalBlock`: The currently known disk block number of the page.
- `FlushedLoc`: The disk block number where the page was written since the last heap fsync.
- `FlushedBeforeSync`: The snapshot of `FlushedLoc` recorded immediately before an active heap fsync begins.
- `DurableLoc`: The disk block number confirmed durable by a completed heap fsync.

These fields operate as follows:
1. When a dirty page flushes to disk, the buddy allocator reserves a new block run. The page writes to this new location. The engine sets `desc.FlushedLoc` and `desc.PhysicalBlock` to the new block number. If `desc.FlushedLoc` previously held an older block number, the engine frees that block number immediately only if it is neither `desc.DurableLoc` nor `desc.FlushedBeforeSync`. The `FlushedBeforeSync` exception keeps a copy alive while a heap fsync for it is in progress. The engine leaves `desc.DurableLoc` untouched.
2. During a checkpoint or explicit sync:
   - The engine calls `bp.SnapshotFlushedForSync()`. This copies `FlushedLoc` to `FlushedBeforeSync` only for pages without a flush in flight (`!desc.InFlight`). Skipping pages with a flush in flight is safe because their write may not be complete when the heap fsync starts; the engine leaves their `FlushedBeforeSync` as `NoBlock`, and a subsequent sync promotes their copy once it has flushed.
   - The engine invokes `fsync` on the heap file.
   - The engine calls `bp.PromoteFlushedToDurable()`. This promotes `FlushedBeforeSync` to `DurableLoc`. If an older `DurableLoc` existed, that older block number is now superseded. The engine frees the old block number to the buddy allocator.
3. If new writes flush during the heap fsync, their new block numbers populate `FlushedLoc`. Those writes remain un-promoted until the subsequent fsync completes.
4. If the system crashes before the heap fsync completes, `DurableLoc` still holds the valid previous copy. Recovery restores that copy.

### 3. Natural Alignment of Block Runs

Every block run of order $k$ ($2^k$ blocks) allocated by the buddy allocator begins at a block number that is a multiple of $2^k$.

When the block pool flushes a batch of pages together, `AllocateBatch` sorts page requests from largest order to smallest order. This descending order packs pages into a single block run while preserving natural alignment for each page.

Crash recovery depends on natural alignment for three reasons:
1. **Candidate rejection:** When the recovery scanner checks block 0 of a candidate page with block count $N = 2^k$, it checks whether `blockNumber % N == 0`. It rejects unaligned blocks immediately. This prevents leftover blocks, inner blocks of larger runs, and payload bytes from masquerading as page starts.
2. **Run verification:** Natural alignment ensures that candidate runs do not cross allocation boundaries.
3. **Buddy allocator reconstruction:** The buddy allocator uses binary address calculations (`block ^ (1 << order)`) to split and coalesce blocks. These calculations require natural alignment. Recovery rebuilds the allocator free lists by marking winning candidates with `buddy.MarkAllocated`. Unaligned block numbers would corrupt the buddy tree structure.

### 4. Immutability of Fsynced WAL Blocks

The engine never rewrites a WAL block once it has been fsynced to disk.

When the engine syncs the active WAL segment (`Sync` or `SyncThrough`), the write cursor may not align with a 4 KiB block boundary (`appendOff % 4096 != 0`). In this case, the engine pads the remainder of the 4 KiB block with zeros in memory. It advances `appendOff` to the start of the next 4 KiB block.

The engine writes and fsyncs the padded block to disk. Subsequent appends write to the next block boundary. The engine never modifies the padded block again.

Rewriting a fsynced block would risk torn writes during a power failure. An interrupted write could corrupt durable records that earlier callers already confirmed. Zero-padding ensures that fsynced blocks remain immutable on disk. During recovery, the WAL iterator recognizes trailing zeros in a block as padding and advances to the next 4 KiB block boundary.

### 5. Retention of Logical Page Numbers

The engine never releases or destroys logical page numbers.

The engine assigns logical page numbers as positive 64-bit integers from a monotonic counter (`nextLogicalPage++`). When all records in a slotted page or multi-block page are deleted, the engine does not discard the logical page number. The engine adds the empty page number to an empty logical page list.

When a write needs a new page, the engine takes an empty logical page number from the list before incrementing `nextLogicalPage`. Recovery discovers empty pages on disk and populates the empty page list. Logical page numbers never shrink, and their identities remain stable.

## Recovery

The engine recovers database state through the following steps in `recovery.go`:

1. **Directory and file fsync:** The engine fsyncs all existing WAL segments, the WAL directory, and the heap file. This ensures that earlier operating system writeback completes before recovery scans the files.
2. **Physical heap block scan:** The engine reads the heap file sequentially in 256 KiB chunks (64 blocks). For each 4 KiB block:
   - The engine validates the magic byte (`0x47`), format version (`1`), and CRC32C.
   - For block index 0, the engine verifies natural alignment (`blockNumber % numBlocks == 0`).
   - The engine verifies that subsequent blocks in the run contain matching logical page numbers, LSNs, and sequential block indices.
   - The engine discards candidate runs with missing, corrupt, or torn blocks.
   - For each logical page number, the engine keeps the candidate run with the highest LSN.
3. **Buddy allocator reconstruction:** The engine marks all winning block runs as allocated in the buddy allocator (`buddy.MarkAllocated`).
4. **Index and empty page reconstruction:** The engine reads the winning pages in physical block order:
   - For slotted pages, the engine scans active record slots and populates the in-memory key index with `(logicalPage, slot, LSN)` entries. If a slotted page contains zero active records, the engine adds the logical page number to the empty page list.
   - For multi-block pages, the engine reads record metadata from block 0. It registers active records in the key index at record slot 0. It registers tombstones in the empty page list.
5. **WAL checkpoint scan:** The engine scans WAL records to find the most recent checkpoint record and extract its `RedoStartLSN`. The engine also reconstructs the highest progress token for each bucket.
6. **WAL redo replay:** The engine replays WAL records from `RedoStartLSN` forward. If a record's LSN is greater than the target page's recovered LSN, the engine applies the mutation directly to an in-memory page buffer and marks the page dirty.
7. **Redo flush:** The engine flushes all dirty pages modified during redo replay to new heap locations allocated from the buddy allocator. It frees their superseded locations and fsyncs the heap file.
8. **Runtime state initialization:** The engine returns the recovered page table, buddy allocator, key index, empty logical pages, progress tokens, and next logical page number.

## Concurrency

The engine coordinates concurrent operations through the following locks:

- `writerMu` (`sync.RWMutex` on `Engine`):
  - Exclusive lock (`Lock`) taken by mutating operations (`Put`, `Delete`) to serialize writes.
  - Shared lock (`RLock`) taken by read scans (`Scan`, `List`) to provide a consistent point-in-time snapshot during traversal.
- `keyMu` (`sync.RWMutex` on `Engine`):
  - Protects the in-memory key index map and oversize object store.
  - Acquired with `RLock` during point lookups (`Get`) and index iterations.
  - Acquired with `Lock` when inserting, updating, or deleting key index entries.
  - Acquired and released within the scope of `writerMu`. The engine never attempts to acquire `writerMu` while holding `keyMu`.
- `BufferPool.mu` (`sync.Mutex` on `BufferPool`):
  - Protects block pool metadata, the page table map, the free block list, and dirty page lists.
  - The engine never acquires `BufferPool.mu` while holding a per-page latch (`desc.Mu`).
- `PageDesc.Mu` (`sync.RWMutex` on `PageDesc`):
  - The per-page latch protecting read and write access to the page's block memory in the pool arena.
  - Callers pin the page under `BufferPool.mu`, release `BufferPool.mu`, and then acquire `PageDesc.Mu`.
- `BufferPool.flushMu` (`sync.Mutex` on `BufferPool`):
  - Serializes access to the direct I/O staging buffer and writes to the heap file.
  - Acquired during page flushes after releasing `BufferPool.mu`.
- `BuddyAllocator.mu` (`sync.Mutex` on `BuddyAllocator`):
  - Protects buddy free lists and allocation mappings.
  - Acquired during block allocation, batch allocation, and block deallocation.
- `WAL.appendMu` (`sync.Mutex` on `WAL`):
  - Serializes record appenders so records append without interleaving.
- `WAL.mu` (`sync.Mutex` on `WAL`):
  - Protects WAL double-buffers, file offsets, LSN tracking, and background flusher coordination.
- `Checkpointer.mu` (`sync.Mutex` on `Checkpointer`):
  - Serializes checkpoint operations so only one checkpoint executes at a time.
  - Acquired without holding `writerMu`, which allows checkpoints to run concurrently with writes.
- `failMu` (`sync.RWMutex` on `Engine`):
  - Protects the sticky error state and failure channels.

### Lock Nesting and Acquisition

The engine uses the following lock nestings:
- `writerMu` → `keyMu`: Mutating operations (`Put`, `Delete`) acquire `writerMu`, and then acquire `keyMu` when inserting, updating, or deleting index entries. `keyMu` is always innermost and never held across I/O.
- `BufferPool.mu` → `BuddyAllocator.mu`: The block pool acquires `BufferPool.mu` before invoking the buddy allocator to allocate block runs (`AllocateBatch`) or free superseded blocks (`Free`).
- `WAL.appendMu` → `WAL.mu`: The WAL appender acquires `WAL.appendMu` to serialize incoming records, and then acquires `WAL.mu` to reserve buffer space and assign LSNs.
- `Checkpointer.mu` → the pool and WAL locks: The checkpointer acquires `Checkpointer.mu` without holding `writerMu`. Under `Checkpointer.mu`, the checkpointer sequentially invokes operations that acquire `BufferPool.mu` (snapshotting dirty pages and flushed locations), `BufferPool.flushMu` (flushing dirty pages), and the WAL locks (`AppendRecord`, `Sync`, and `TruncateBefore`).

The following locks are taken only after `BufferPool.mu` is released:
- `PageDesc.Mu`: Callers pin the page under `BufferPool.mu`, release `BufferPool.mu`, and only then acquire `PageDesc.Mu` (shared for reads, exclusive for writes or flushes). Callers never acquire `BufferPool.mu` while holding `PageDesc.Mu`.
- `BufferPool.flushMu`: The flusher marks pages in-flight and unpins them under `BufferPool.mu`, releases `BufferPool.mu`, and only then acquires `BufferPool.flushMu` to serialize staging buffer usage and direct I/O writes to the heap file.
