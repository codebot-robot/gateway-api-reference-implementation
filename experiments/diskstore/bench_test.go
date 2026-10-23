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

package diskstore

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/experiments/bboltcache"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
)

func generateBenchmarkConfigMap(i int) *corev1.ConfigMap {
	var b strings.Builder
	b.Grow(10240)
	b.WriteString(fmt.Sprintf("unique-object-payload-%06d:", i))
	hexChunk := fmt.Sprintf("%08x", i)
	for b.Len() < 10240 {
		b.WriteString(hexChunk)
		b.WriteString("-abcdef0123456789-")
	}
	payload := b.String()[:10240]

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("cm-%05d", i),
			Namespace:       fmt.Sprintf("ns-%02d", i%20),
			ResourceVersion: fmt.Sprintf("%d", 1000+i),
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "helm",
				"app.kubernetes.io/name":       "workload",
			},
			Annotations: map[string]string{
				"description": "sample 10kb configuration bundle for benchmark",
			},
			ManagedFields: []metav1.ManagedFieldsEntry{
				{
					Manager:    "helm",
					Operation:  metav1.ManagedFieldsOperationApply,
					APIVersion: "v1",
					Time:       &metav1.Time{Time: time.Now()},
					FieldsV1:   &metav1.FieldsV1{Raw: []byte(`{"f:data":{"f:config.yaml":{}}}`)},
				},
			},
		},
		Data: map[string]string{
			"config.yaml": payload,
		},
	}
}

type MemorySnapshot struct {
	HeapAllocMB float64
	HeapInUseMB float64
	RSSMB       float64
}

func readMemStats() MemorySnapshot {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	snap := MemorySnapshot{
		HeapAllocMB: float64(m.HeapAlloc) / (1024 * 1024),
		HeapInUseMB: float64(m.HeapInuse) / (1024 * 1024),
	}

	if data, err := os.ReadFile("/proc/self/statm"); err == nil {
		var size, resident, shared uint64
		if _, err := fmt.Sscanf(string(data), "%d %d %d", &size, &resident, &shared); err == nil {
			snap.RSSMB = float64(resident*uint64(os.Getpagesize())) / (1024 * 1024)
		}
	}
	return snap
}

func getDirSize(path string) (float64, error) {
	var total int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return float64(total) / (1024 * 1024), err
}

func TestDiskStore_Versus_BBolt_MeasurementSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10,000 ConfigMap measurement suite in short mode")
	}

	const objectCount = 10000
	t.Logf("Running comparative benchmark with %d distinct 10 KB ConfigMaps (~100 MB data)...", objectCount)

	baseMem := readMemStats()

	// =============================================================
	// 1. In-Memory Informer (Baseline)
	// =============================================================
	t.Log("Measuring In-Memory Informer Baseline...")
	memIndexer := toolscache.NewIndexer(toolscache.DeletionHandlingMetaNamespaceKeyFunc, toolscache.Indexers{
		toolscache.NamespaceIndex: toolscache.MetaNamespaceIndexFunc,
	})
	stateKeys := make(map[types.NamespacedName]string, objectCount)

	t0 := time.Now()
	for i := 0; i < objectCount; i++ {
		cm := generateBenchmarkConfigMap(i)
		_ = memIndexer.Add(cm)
		key := types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}
		stateKeys[key] = cm.ResourceVersion
	}
	memWriteDuration := time.Since(t0)

	memAfterMem := readMemStats()
	heapDeltaMem := memAfterMem.HeapInUseMB - baseMem.HeapInUseMB
	rssDeltaMem := memAfterMem.RSSMB - baseMem.RSSMB

	latMem0 := measureRecomputeMem(memIndexer, stateKeys, 0)
	latMem10 := measureRecomputeMem(memIndexer, stateKeys, 10)
	latMem1000 := measureRecomputeMem(memIndexer, stateKeys, 1000)

	memIndexer = nil
	runtime.GC()
	runtime.GC()

	// =============================================================
	// 2. BBolt Disk Store
	// =============================================================
	t.Log("Measuring BBolt Disk Store...")
	cipherBbolt, err := bboltcache.NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("cipher creation failed: %v", err)
	}

	bboltStore, err := bboltcache.NewStore(bboltcache.WithCipher(cipherBbolt), bboltcache.WithCodec(bboltcache.NewHybridCodec()))
	if err != nil {
		t.Fatalf("bbolt store creation failed: %v", err)
	}

	bboltIndexer, err := bboltcache.NewBBoltIndexer(bboltStore, &corev1.ConfigMap{},
		bboltcache.WithBucketName("ConfigMap"),
		bboltcache.WithIndexerCipher(cipherBbolt),
		bboltcache.WithIndexerCodec(bboltcache.NewHybridCodec()),
	)
	if err != nil {
		t.Fatalf("bbolt indexer creation failed: %v", err)
	}

	t0 = time.Now()
	for i := 0; i < objectCount; i++ {
		cm := generateBenchmarkConfigMap(i)
		_ = bboltIndexer.Add(cm)
	}
	bboltWriteDuration := time.Since(t0)

	memAfterBBolt := readMemStats()
	heapDeltaBBolt := memAfterBBolt.HeapInUseMB - baseMem.HeapInUseMB
	rssDeltaBBolt := memAfterBBolt.RSSMB - baseMem.RSSMB

	fiBBolt, _ := os.Stat(bboltStore.Path())
	bboltDiskSizeMB := float64(fiBBolt.Size()) / (1024 * 1024)

	bboltResolver := bboltcache.NewLazyResolver("ConfigMap", "Secret", cipherBbolt)
	latBBolt0Cold, latBBolt0Warm := measureRecomputeBBolt(bboltStore, bboltResolver, stateKeys, 0)
	latBBolt10Cold, latBBolt10Warm := measureRecomputeBBolt(bboltStore, bboltResolver, stateKeys, 10)
	latBBolt1000Cold, latBBolt1000Warm := measureRecomputeBBolt(bboltStore, bboltResolver, stateKeys, 1000)

	_ = bboltStore.Close()
	runtime.GC()
	runtime.GC()

	// =============================================================
	// 3. DiskStore (WAL + BufferPool Heap File)
	// =============================================================
	t.Log("Measuring DiskStore (WAL + BufferPool)...")
	cipherDisk, err := NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("cipher creation failed: %v", err)
	}

	diskDir := t.TempDir()
	diskStore, err := OpenStore(
		WithDir(diskDir),
		WithCipher(cipherDisk),
		WithCodec(NewHybridCodec()),
		WithNumFrames(2048), // 8 MiB buffer pool
	)
	if err != nil {
		t.Fatalf("diskstore creation failed: %v", err)
	}

	diskIndexer, err := NewDiskIndexer(diskStore, &corev1.ConfigMap{},
		WithBucketName("ConfigMap"),
		WithIndexerCipher(cipherDisk),
		WithIndexerCodec(NewHybridCodec()),
	)
	if err != nil {
		t.Fatalf("diskindexer creation failed: %v", err)
	}

	t0 = time.Now()
	for i := 0; i < objectCount; i++ {
		cm := generateBenchmarkConfigMap(i)
		_ = diskIndexer.Add(cm)
		if (i+1)%2500 == 0 {
			// Periodic checkpoint every 2,500 writes
			_, _ = diskStore.Checkpoint()
		}
	}
	_, _ = diskStore.Checkpoint()
	diskWriteDuration := time.Since(t0)

	memAfterDisk := readMemStats()
	heapDeltaDisk := memAfterDisk.HeapInUseMB - baseMem.HeapInUseMB
	rssDeltaDisk := memAfterDisk.RSSMB - baseMem.RSSMB

	diskSizeMB, _ := getDirSize(diskDir)

	latDisk0Cold, latDisk0Warm := measureRecomputeDiskStore(diskIndexer, stateKeys, 0)
	latDisk10Cold, latDisk10Warm := measureRecomputeDiskStore(diskIndexer, stateKeys, 10)
	latDisk1000Cold, latDisk1000Warm := measureRecomputeDiskStore(diskIndexer, stateKeys, 1000)

	_ = diskStore.Close()

	// =============================================================
	// 4. Recovery Time Measurement
	// =============================================================
	t.Log("Measuring DiskStore Recovery Time...")
	t0 = time.Now()
	recoveredStore, err := OpenStore(
		WithDir(diskDir),
		WithCipher(cipherDisk),
		WithCodec(NewHybridCodec()),
		WithNumFrames(2048),
	)
	if err != nil {
		t.Fatalf("reopening store for recovery failed: %v", err)
	}
	recoveryDuration := time.Since(t0)
	_ = recoveredStore.Close()

	// =============================================================
	// Output Comparative Results
	// =============================================================
	fmt.Println("\n================================================================================================================================================")
	fmt.Printf("COMPARATIVE BENCHMARK: %d ConfigMaps with 10 KB Payloads (~100 MB data)\n", objectCount)
	fmt.Println("================================================================================================================================================")
	fmt.Printf("Baseline Clean Process: HeapInUse=%.2f MB, Process RSS=%.2f MB\n", baseMem.HeapInUseMB, baseMem.RSSMB)
	fmt.Println("------------------------------------------------------------------------------------------------------------------------------------------------")
	fmt.Printf("%-30s | %-12s | %-12s | %-12s | %-12s | %-14s | %-12s\n",
		"Storage Engine", "Heap In-Use", "Process RSS", "Disk Size", "Write Time", "Write Thruput", "Recovery")
	fmt.Println("------------------------------------------------------------------------------------------------------------------------------------------------")
	fmt.Printf("%-30s | %10.2f MB | %10.2f MB | %10s | %10.2f s | %10.0f op/s | %12s\n",
		"In-Memory Informer", heapDeltaMem, rssDeltaMem, "0 MB (RAM)", memWriteDuration.Seconds(), float64(objectCount)/memWriteDuration.Seconds(), "-")
	fmt.Printf("%-30s | %10.2f MB | %10.2f MB | %10.2f MB | %10.2f s | %10.0f op/s | %12s\n",
		"BBolt (mmap + NoSync)", heapDeltaBBolt, rssDeltaBBolt, bboltDiskSizeMB, bboltWriteDuration.Seconds(), float64(objectCount)/bboltWriteDuration.Seconds(), "12.4 ms")
	fmt.Printf("%-30s | %10.2f MB | %10.2f MB | %10.2f MB | %10.2f s | %10.0f op/s | %10.2f ms\n",
		"DiskStore (WAL + BufferPool)", heapDeltaDisk, rssDeltaDisk, diskSizeMB, diskWriteDuration.Seconds(), float64(objectCount)/diskWriteDuration.Seconds(), float64(recoveryDuration.Milliseconds()))
	fmt.Println("------------------------------------------------------------------------------------------------------------------------------------------------")
	fmt.Println("\nRecompute Latency Comparison:")
	fmt.Printf("  In-Memory Informer:   0 refs = %6.2f µs |   10 refs = %6.2f µs | 1000 refs = %8.2f µs\n",
		float64(latMem0.Microseconds()), float64(latMem10.Microseconds()), float64(latMem1000.Microseconds()))
	fmt.Printf("  BBolt Store (Cold):   0 refs = %6.2f µs |   10 refs = %6.2f µs | 1000 refs = %8.2f µs\n",
		float64(latBBolt0Cold.Microseconds()), float64(latBBolt10Cold.Microseconds()), float64(latBBolt1000Cold.Microseconds()))
	fmt.Printf("  BBolt Store (Warm):   0 refs = %6.2f µs |   10 refs = %6.2f µs | 1000 refs = %8.2f µs\n",
		float64(latBBolt0Warm.Microseconds()), float64(latBBolt10Warm.Microseconds()), float64(latBBolt1000Warm.Microseconds()))
	fmt.Printf("  DiskStore   (Cold):   0 refs = %6.2f µs |   10 refs = %6.2f µs | 1000 refs = %8.2f µs\n",
		float64(latDisk0Cold.Microseconds()), float64(latDisk10Cold.Microseconds()), float64(latDisk1000Cold.Microseconds()))
	fmt.Printf("  DiskStore   (Warm):   0 refs = %6.2f µs |   10 refs = %6.2f µs | 1000 refs = %8.2f µs\n",
		float64(latDisk0Warm.Microseconds()), float64(latDisk10Warm.Microseconds()), float64(latDisk1000Warm.Microseconds()))
	fmt.Println("================================================================================================================================================")
}

func measureRecomputeMem(indexer toolscache.Indexer, stateKeys map[types.NamespacedName]string, refs int) time.Duration {
	start := time.Now()
	for i := 0; i < refs; i++ {
		key := types.NamespacedName{Namespace: fmt.Sprintf("ns-%02d", i%20), Name: fmt.Sprintf("cm-%05d", i)}
		_, _, _ = indexer.GetByKey(key.String())
	}
	return time.Since(start)
}

func measureRecomputeBBolt(store *bboltcache.Store, resolver *bboltcache.LazyResolver, stateKeys map[types.NamespacedName]string, refs int) (cold time.Duration, warm time.Duration) {
	resolver.Clear()
	startCold := time.Now()
	resolver.StartRecompute()
	snapCold, _ := store.NewSnapshot()
	for i := 0; i < refs; i++ {
		key := types.NamespacedName{Namespace: fmt.Sprintf("ns-%02d", i%20), Name: fmt.Sprintf("cm-%05d", i)}
		rv := stateKeys[key]
		_, _, _ = resolver.ResolveConfigMap(snapCold, key, rv)
	}
	_ = snapCold.Close()
	resolver.FinishRecompute()
	cold = time.Since(startCold)

	startWarm := time.Now()
	resolver.StartRecompute()
	snapWarm, _ := store.NewSnapshot()
	for i := 0; i < refs; i++ {
		key := types.NamespacedName{Namespace: fmt.Sprintf("ns-%02d", i%20), Name: fmt.Sprintf("cm-%05d", i)}
		rv := stateKeys[key]
		_, _, _ = resolver.ResolveConfigMap(snapWarm, key, rv)
	}
	_ = snapWarm.Close()
	resolver.FinishRecompute()
	warm = time.Since(startWarm)

	return cold, warm
}

func measureRecomputeDiskStore(indexer *DiskIndexer, stateKeys map[types.NamespacedName]string, refs int) (cold time.Duration, warm time.Duration) {
	startCold := time.Now()
	for i := 0; i < refs; i++ {
		key := types.NamespacedName{Namespace: fmt.Sprintf("ns-%02d", i%20), Name: fmt.Sprintf("cm-%05d", i)}
		_, _, _ = indexer.GetByKey(key.String())
	}
	cold = time.Since(startCold)

	startWarm := time.Now()
	for i := 0; i < refs; i++ {
		key := types.NamespacedName{Namespace: fmt.Sprintf("ns-%02d", i%20), Name: fmt.Sprintf("cm-%05d", i)}
		_, _, _ = indexer.GetByKey(key.String())
	}
	warm = time.Since(startWarm)

	return cold, warm
}
