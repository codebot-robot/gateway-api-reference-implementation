# Experiment: Disk-Backed (bbolt) Informer Store for ConfigMaps and Secrets

## Executive Summary

In controller mode, GARI watches every `ConfigMap` and `Secret` in the cluster to support Gateway API references:
- GatewayClass and Gateway `parametersRef` (Issue #767)
- `BackendTLSPolicy` client validation CA certificates
- Frontend client-validation CA certificate bundles
- Gateway listener TLS certificate and private key Secrets

On large shared clusters with thousands of ConfigMaps and Secrets (each up to 1 MiB), holding all objects in the Go heap—once in the controller-runtime informer cache and once as deep copies in `State` snapshots—becomes the dominant memory cost, even though only a tiny fraction (often 1–5 objects) is ever referenced.

This prototype spike implements and measures an on-disk store backed by **bbolt** (`go.etcd.io/bbolt`) with:
1. **Ephemeral AEAD Encryption**: Random 256-bit AES-GCM key generated in-memory at startup (`crypto/rand`). Ciphertexts are cryptographically bound to their bucket and key via AEAD additional authenticated data (`additionalData = bucketName + "/" + key`), preventing key-swapping attacks.
2. **Protobuf & Metadata Pruning Codec**: Core Kubernetes types (`ConfigMap`, `Secret`) are encoded using their native protobuf serializers (`Marshal`/`Unmarshal`), falling back to JSON for custom types. Bulky `managedFields` are stripped from a cloned copy on serialization without mutating the informer's original object.
3. **Optimized Ephemeral Scratch I/O**: bbolt is opened with `NoSync: true`, `NoFreelistSync: true`, and `InitialMmapSize: 256MB`, eliminating disk sync latency while avoiding mmap lock pauses.
4. **`client-go` `cache.Indexer` Compatibility**: A complete `cache.Indexer` implementation (`BBoltIndexer`) backed by bbolt buckets and secondary B+tree index buckets, ensuring standard informer semantics and transparent `client.Get`/`client.List` support.
5. **Broad Read Guardrails**: Broad decodes across entire buckets (`List()` and full-bucket index lookups) are logged at `Info` level with caller stack traces to make any potential heap regressions immediately visible.
6. **Consistent Lazy Snapshot Resolution & Memo Eviction**: Each recompute opens a single point-in-time bbolt read transaction (`Snapshot`), lazily decoding only referenced objects. The `LazyResolver` memoizes active objects by `key@resourceVersion` during a recompute cycle and evicts old versions and unreferenced objects at the end of each cycle via `FinishRecompute()`.

---

## Architecture Framing

1. **Architecture A (Status Quo)**:
   - In-memory `client-go` Informer cache (`toolscache.NewIndexer`).
   - `State` holds deep copies of all objects in `s.configMaps` and `s.secrets`.
   - Recompute copies the full map into `ModelInputs`.

2. **Architecture B (Complementary: Minimal State Logic)**:
   - Not an alternative to the disk-backed store, but a complementary pattern: `State` keeps only its own minimal copies of the specific fields it actually needs (or `key → resourceVersion` references), avoiding full-object heap copies in `State`. However, the underlying informer cache still holds all cluster objects in the Go heap unless backed by disk.

3. **Architecture C (BBolt Disk Store + Protobuf + AES-GCM + Keys in State + Lazy Snapshot Resolver)**:
   - `BBoltIndexer` stores serialized, encrypted objects and B+tree secondary indices on disk.
   - Core types serialized with Protobuf; unneeded `managedFields` pruned on write.
   - `State` holds `key → resourceVersion`.
   - Recompute opens a bbolt read transaction (`Snapshot`) and lazily resolves/memoizes referenced objects using `LazyResolver`.
   - Unreferenced entries and stale versions are purged at the end of each recompute cycle.

---

## Empirical Benchmark Results

Measurements taken on AMD EPYC (Linux x86_64) using `10,000 ConfigMaps` with **distinct 10 KB payloads** each (~100 MB of unique cluster configuration data):

| Architecture | Go Heap In-Use | Delta Process RSS | Disk Size | Recompute (0 refs) | Recompute (10 refs) | Recompute (1,000 refs) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **A: In-Memory + Full State (Status Quo)** | **161.19 MB** | 199.36 MB | 0 MB (RAM) | 1,396 µs | 782 µs | 1,045 µs |
| **B: In-Memory + Keys State** | **147.63 MB** | 175.39 MB | 0 MB (RAM) | 1,406 µs | 1,152 µs | 1,339 µs |
| **C: BBolt + Proto + AES (Cold)** | **1.75 MB** | 147.62 MB | **144.23 MB** | **8 µs** | **183 µs** | **20,030 µs (20 ms)** |
| **C: BBolt + Proto + AES (Warm Memoized)** | **1.75 MB** | 147.62 MB | **144.23 MB** | **0 µs** | **5 µs** | **559 µs** |
| **C: BBolt + JSON + AES (Cold)** | 1.75 MB | 147.62 MB | 144.24 MB | 8 µs | 390 µs | **30,650 µs (30 ms)** |

### Warm Lazy Resolution Microbenchmark
```
BenchmarkLazyResolver_Resolve1000-180    2174    535,035 ns/op    243,368 B/op    9766 allocs/op
```
Warm resolution (cache hit on `key@resourceVersion`) costs approximately **535 ns per resolved object**.

### Key Observations
1. **Heap Elimination**: With distinct 10 KB strings per object, Architecture A consumes **161.19 MB** of Go heap and +199 MB of resident memory (RSS). Architecture C drops Go heap usage down to **1.75 MB** (a **98.9% heap reduction**), storing only lightweight string keys in State while objects reside in bbolt mmap storage.
2. **Protobuf vs JSON**:
   - Cold decoding 1,000 objects from bbolt with Protobuf takes **20.0 ms**, compared to **30.6 ms** for JSON (~35% faster cold resolution).
   - Protobuf encoding produces compact, predictable byte streams ideal for encrypted B+tree storage.
3. **Recompute Latency in Common Cases (0–10 refs)**:
   - In status quo (A), recompute takes ~800–1,400 µs even when 0 ConfigMaps are referenced because the controller must copy and iterate 10,000 map entries every cycle.
   - In BBolt (C), recompute with 0 references takes **8 µs** cold and **0 µs** warm (>150x faster).
   - With 10 referenced objects (typical production Gateway), BBolt recompute takes **183 µs** cold and **5 µs** warm (4x to 150x faster than in-memory).
4. **Memo Eviction**: By tracking `touched` entries during each recompute cycle and calling `FinishRecompute()`, `LazyResolver` guarantees that stale versions and unreferenced objects are dropped immediately, preventing memo cache bloat over time.

---

## Detailed Answers to Review and Architectural Questions

### 1. Heap and RSS Accounting under Cgroup v2
- **Cgroup v2 Accounting**: Under cgroup v2, `memory.current` tracks all memory charged to the container:
  $$\text{memory.current} = \text{anon} + \text{file} + \text{sock}$$
  bbolt memory-mapped pages are charged under `file` (page cache).
- **Eviction vs OOM**: Unlike anonymous heap memory (`anon`), clean file-backed page cache (`file`) is **reclaimable** by the Linux kernel under memory pressure without triggering an OOM kill. If the container reaches its memory limit, the kernel drops clean bbolt mmap pages before invoking the OOM killer on the Go runtime.
- **Node Disk `emptyDir` vs `tmpfs`**:
  - **Node disk `emptyDir` (Recommended)**: Backed by the host filesystem (ext4/xfs). Pages are reclaimable file page cache. Because Secrets are encrypted via in-memory AES-256-GCM, storing them on node disk is cryptographically secure.
  - **`emptyDir.medium: Memory` (tmpfs) (Discouraged)**: tmpfs pages are accounted as `shmem` / anonymous memory in cgroup v2 and are **not** reclaimable without swap. Storing bbolt on tmpfs defeats the purpose of offloading memory from the pod limit.

### 2. Peak Memory during Initial LIST and Relists
- During controller startup or a relist after an HTTP 410 Gone, client-go issues an initial LIST to the API server. In older Kubernetes versions, a large LIST could temporarily spike memory as the entire list is received into memory before being indexed.
- Modern Kubernetes clusters support **streaming watch-list** (`WatchListClient` feature gate enabled by default in Kubernetes 1.27+). GARI relies on streaming watch-list to stream items sequentially into the Reflector and DeltaFIFO without buffering the entire cluster state in memory.

### 3. bbolt Mmap Resizing and Growth Latency
- When a bbolt file grows beyond its memory map, bbolt must unmap and re-mmap the file. To do so, the writer acquires `db.mmaplock.Lock()`, which waits for all open read transactions to finish.
- While a brief pause is completely benign for an asynchronous Kubernetes controller recompute loop, pauses during recompute can be virtually eliminated by setting `InitialMmapSize`:
  - Default set to **256 MB** (`InitialMmapSize: 256 * 1024 * 1024`).
  - Because mmap only reserves virtual address space without consuming physical memory until pages are written, 256 MB comfortably accommodates ~20,000 ConfigMaps/Secrets without triggering re-map lock acquisition.

### 4. Ephemeral Storage Sizing for Operators
- Operators should raise the pod's `ephemeral-storage` request and limit where they previously raised memory:
  - 10,000 ConfigMaps (10 KB each) consume ~145 MB of ephemeral disk storage.
  - On Google Kubernetes Engine (GKE) Autopilot, the default ephemeral-storage limit is **1 GiB**, which can store ~60,000+ 10 KB ConfigMaps/Secrets without configuration changes.
  - For standard clusters, setting an `emptyDir` with a 1–2 GiB size limit provides ample headroom.

### 5. Security: Encrypted Payloads and Plaintext Keys
- **AEAD Encryption**: Secrets and ConfigMaps stored on disk have their payload encrypted with AES-256-GCM. The 256-bit key is generated randomly in-memory at process startup and is never written to disk. When the container restarts or terminates, the scratch file becomes permanently undecryptable.
- **Key-Binding**: Each encryption operation binds `additionalData = bucketName + "/" + key`. If an attacker or corrupted process copies ciphertext from one key to another, decryption fails authentication.
- **Plaintext Object Metadata**: Object keys (`namespace/name`), bucket names, and secondary index values (e.g. namespace names in `_idx_*` buckets) are stored unencrypted to allow cursor seeks, prefix scans, and index lookups. Operators should note that metadata (names and namespaces) is visible in the scratch file, while all sensitive configuration data and TLS private keys are strictly encrypted.

### 6. Broad Reads and Call Sites in GARI
Broad decodes across an entire bucket (`List()` or full-bucket `ByIndex()`) decode every stored object into the Go heap, recreating the heap pressure this store is designed to prevent.
- `BBoltIndexer` logs broad reads at `Info` level along with the caller's stack frame:
  ```
  bboltcache: broad read invoked on bucket "*v1.ConfigMap": List() decoded 10000 objects (caller: ...)
  ```
- **Audit of GARI Call Sites Today**:
  - **Controller Reconcilers (`pkg/controller/`)**: Never call `client.List` for ConfigMaps or Secrets. They only watch events and dispatch key updates.
  - **State (`pkg/state/state.go`)**: Only accesses ConfigMaps and Secrets referenced by Gateway API resources during `recompute()`.
  - **SinglePod Provisioning (`pkg/provisioning/singlepod/`)**: Reads only the specific parameters ConfigMap referenced by `parametersRef`.
  - **Legacy/Test Helpers (`pkg/state/compiled.go`)**: `CompileFromState` contains test helpers calling `s.GetConfigMaps()` and `s.GetSecrets()`. Production code paths never perform broad reads.

---

## Production Roadmap for Follow-on Issue

1. **Graduate Package**: Move `experiments/bboltcache` to `pkg/store/bbolt`.
2. **Add CLI Flag**: Introduce `--experimental-disk-cache` in `cmd/gateway-api-reference-implementation`.
3. **Controller-Runtime Informer Override**:
   Configure `cache.Options.NewInformer` for `corev1.ConfigMap` and `corev1.Secret` to return informers backed by `BBoltIndexer`.
4. **State Snapshot Integration**:
   - Provide `bboltcache.Store` to `State`.
   - In `State.UpsertConfigMap` and `State.UpsertSecret`, store only `key → resourceVersion` when disk cache is active.
   - During `recompute()`, open `store.NewSnapshot()`, resolve referenced objects through `LazyResolver`, and call `FinishRecompute()` to purge inactive entries.
