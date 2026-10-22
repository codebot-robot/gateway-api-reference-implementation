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

package bboltcache

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
)

// generateConfigMap creates a ConfigMap with a distinct 10 KB payload.
func generateConfigMap(i int) *corev1.ConfigMap {
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

// MemorySnapshot holds memory metrics including Go heap, process RSS, and cgroup memory.
type MemorySnapshot struct {
	HeapAllocMB float64
	HeapInUseMB float64
	RSSMB       float64
	CgroupMB    float64
}

func readMemoryStats() MemorySnapshot {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	snap := MemorySnapshot{
		HeapAllocMB: float64(m.HeapAlloc) / (1024 * 1024),
		HeapInUseMB: float64(m.HeapInuse) / (1024 * 1024),
	}

	// Read process RSS from /proc/self/statm
	if data, err := os.ReadFile("/proc/self/statm"); err == nil {
		var size, resident, shared uint64
		if _, err := fmt.Sscanf(string(data), "%d %d %d", &size, &resident, &shared); err == nil {
			snap.RSSMB = float64(resident*uint64(os.Getpagesize())) / (1024 * 1024)
		}
	}

	// Read cgroup v2 memory.current (or cgroup v1 memory.usage_in_bytes)
	if data, err := os.ReadFile("/sys/fs/cgroup/memory.current"); err == nil {
		var b uint64
		if _, err := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &b); err == nil {
			snap.CgroupMB = float64(b) / (1024 * 1024)
		}
	} else if data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.usage_in_bytes"); err == nil {
		var b uint64
		if _, err := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &b); err == nil {
			snap.CgroupMB = float64(b) / (1024 * 1024)
		}
	}

	return snap
}

// TestMeasurementSuite runs the implementation spikes and outputs empirical results.
func TestMeasurementSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping measurement suite in short mode")
	}

	const objectCount = 10000
	t.Logf("Running benchmark with %d distinct 10 KB ConfigMaps (~100 MB data)...", objectCount)

	baseMem := readMemoryStats()

	// -------------------------------------------------------------
	// 1. Implementation A: In-Memory Informer + Full Objects in State (Status Quo)
	// -------------------------------------------------------------
	t.Log("Measuring Implementation A: In-Memory Cache + Full Objects in State...")
	memIndexerA := toolscache.NewIndexer(toolscache.DeletionHandlingMetaNamespaceKeyFunc, toolscache.Indexers{
		toolscache.NamespaceIndex: toolscache.MetaNamespaceIndexFunc,
	})
	stateObjectsA := make(map[types.NamespacedName]*corev1.ConfigMap, objectCount)

	for i := 0; i < objectCount; i++ {
		cm := generateConfigMap(i)
		_ = memIndexerA.Add(cm)
		key := types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}
		stateObjectsA[key] = cm.DeepCopy() // Status quo deep copies full object into State
	}

	memAfterA := readMemoryStats()
	heapDeltaA := memAfterA.HeapInUseMB - baseMem.HeapInUseMB
	rssDeltaA := memAfterA.RSSMB - baseMem.RSSMB

	latA0 := measureRecomputeA(stateObjectsA, 0)
	latA10 := measureRecomputeA(stateObjectsA, 10)
	latA1000 := measureRecomputeA(stateObjectsA, 1000)

	memIndexerA = nil
	stateObjectsA = nil
	runtime.GC()
	runtime.GC()

	// -------------------------------------------------------------
	// 2. Implementation B: In-Memory Informer + Keys-only in State
	// -------------------------------------------------------------
	t.Log("Measuring Implementation B: In-Memory Cache + Keys-only in State...")
	memIndexerB := toolscache.NewIndexer(toolscache.DeletionHandlingMetaNamespaceKeyFunc, toolscache.Indexers{
		toolscache.NamespaceIndex: toolscache.MetaNamespaceIndexFunc,
	})
	stateKeysB := make(map[types.NamespacedName]string, objectCount)

	for i := 0; i < objectCount; i++ {
		cm := generateConfigMap(i)
		_ = memIndexerB.Add(cm)
		key := types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}
		stateKeysB[key] = cm.ResourceVersion
	}

	memAfterB := readMemoryStats()
	heapDeltaB := memAfterB.HeapInUseMB - baseMem.HeapInUseMB
	rssDeltaB := memAfterB.RSSMB - baseMem.RSSMB

	latB0 := measureRecomputeB(memIndexerB, stateKeysB, 0)
	latB10 := measureRecomputeB(memIndexerB, stateKeysB, 10)
	latB1000 := measureRecomputeB(memIndexerB, stateKeysB, 1000)

	memIndexerB = nil
	stateKeysB = nil
	runtime.GC()
	runtime.GC()

	// -------------------------------------------------------------
	// 3. Implementation C (Protobuf): BBolt Disk Store + Protobuf Codec + AES-GCM + Keys-only in State
	// -------------------------------------------------------------
	t.Log("Measuring Implementation C (Protobuf): BBolt Store + Protobuf + AES-GCM...")
	cipherProto, err := NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("cipher creation failed: %v", err)
	}

	storeProto, err := NewStore(WithCipher(cipherProto), WithCodec(NewHybridCodec()))
	if err != nil {
		t.Fatalf("store creation failed: %v", err)
	}
	defer storeProto.Close()

	bboltIndexerProto, err := NewBBoltIndexer(storeProto, &corev1.ConfigMap{},
		WithBucketName("ConfigMap"),
		WithIndexerCipher(cipherProto),
		WithIndexerCodec(NewHybridCodec()),
		WithIndexers(toolscache.Indexers{
			toolscache.NamespaceIndex: toolscache.MetaNamespaceIndexFunc,
		}),
	)
	if err != nil {
		t.Fatalf("indexer creation failed: %v", err)
	}

	stateKeysCProto := make(map[types.NamespacedName]string, objectCount)
	for i := 0; i < objectCount; i++ {
		cm := generateConfigMap(i)
		_ = bboltIndexerProto.Add(cm)
		key := types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}
		stateKeysCProto[key] = cm.ResourceVersion
	}

	memAfterCProto := readMemoryStats()
	heapDeltaCProto := memAfterCProto.HeapInUseMB - baseMem.HeapInUseMB
	rssDeltaCProto := memAfterCProto.RSSMB - baseMem.RSSMB

	fiProto, _ := os.Stat(storeProto.Path())
	diskSizeProtoMB := float64(fiProto.Size()) / (1024 * 1024)

	resolverProto := NewLazyResolver("ConfigMap", "Secret", cipherProto)

	latC0ColdProto, latC0WarmProto := measureRecomputeC(storeProto, resolverProto, stateKeysCProto, 0)
	latC10ColdProto, latC10WarmProto := measureRecomputeC(storeProto, resolverProto, stateKeysCProto, 10)
	latC1000ColdProto, latC1000WarmProto := measureRecomputeC(storeProto, resolverProto, stateKeysCProto, 1000)

	// -------------------------------------------------------------
	// 4. Implementation C (JSON): BBolt Disk Store + JSON Codec + AES-GCM (for comparison)
	// -------------------------------------------------------------
	t.Log("Measuring Implementation C (JSON): BBolt Store + JSON + AES-GCM for comparison...")
	cipherJSON, err := NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("cipher creation failed: %v", err)
	}

	storeJSON, err := NewStore(WithCipher(cipherJSON), WithCodec(NewJSONCodec()))
	if err != nil {
		t.Fatalf("store creation failed: %v", err)
	}
	defer storeJSON.Close()

	bboltIndexerJSON, err := NewBBoltIndexer(storeJSON, &corev1.ConfigMap{},
		WithBucketName("ConfigMap"),
		WithIndexerCipher(cipherJSON),
		WithIndexerCodec(NewJSONCodec()),
	)
	if err != nil {
		t.Fatalf("indexer creation failed: %v", err)
	}

	stateKeysCJSON := make(map[types.NamespacedName]string, objectCount)
	for i := 0; i < objectCount; i++ {
		cm := generateConfigMap(i)
		_ = bboltIndexerJSON.Add(cm)
		key := types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}
		stateKeysCJSON[key] = cm.ResourceVersion
	}

	fiJSON, _ := os.Stat(storeJSON.Path())
	diskSizeJSONMB := float64(fiJSON.Size()) / (1024 * 1024)

	resolverJSON := NewLazyResolver("ConfigMap", "Secret", cipherJSON)
	latC1000ColdJSON, _ := measureRecomputeC(storeJSON, resolverJSON, stateKeysCJSON, 1000)

	// Print Summary Report Table
	fmt.Println("\n========================================================================================================================")
	fmt.Printf("EMPIRICAL MEASUREMENTS: N = %d ConfigMaps with Distinct 10 KB Payloads (~100 MB data)\n", objectCount)
	fmt.Println("========================================================================================================================")
	fmt.Printf("Baseline (Clean Process): HeapInUse=%.2f MB, Process RSS=%.2f MB, cgroup memory=%.2f MB\n",
		baseMem.HeapInUseMB, baseMem.RSSMB, baseMem.CgroupMB)
	fmt.Println("------------------------------------------------------------------------------------------------------------------------")
	fmt.Printf("%-36s | %-12s | %-12s | %-12s | %-12s | %-12s\n", "Architecture", "Heap In-Use", "Delta RSS", "Disk Size", "Recomp (0)", "Recomp (1000)")
	fmt.Println("------------------------------------------------------------------------------------------------------------------------")
	fmt.Printf("%-36s | %10.2f MB | %10.2f MB | %10s | %10.2f µs | %10.2f µs\n",
		"A: In-Memory + Full State", heapDeltaA, rssDeltaA, "0 MB (RAM)", float64(latA0.Microseconds()), float64(latA1000.Microseconds()))
	fmt.Printf("%-36s | %10.2f MB | %10.2f MB | %10s | %10.2f µs | %10.2f µs\n",
		"B: In-Memory + Keys State", heapDeltaB, rssDeltaB, "0 MB (RAM)", float64(latB0.Microseconds()), float64(latB1000.Microseconds()))
	fmt.Printf("%-36s | %10.2f MB | %10.2f MB | %10.2f MB | %10.2f µs | %10.2f µs\n",
		"C: BBolt + Proto + AES (Cold)", heapDeltaCProto, rssDeltaCProto, diskSizeProtoMB, float64(latC0ColdProto.Microseconds()), float64(latC1000ColdProto.Microseconds()))
	fmt.Printf("%-36s | %10.2f MB | %10.2f MB | %10.2f MB | %10.2f µs | %10.2f µs\n",
		"C: BBolt + Proto + AES (Warm)", heapDeltaCProto, rssDeltaCProto, diskSizeProtoMB, float64(latC0WarmProto.Microseconds()), float64(latC1000WarmProto.Microseconds()))
	fmt.Printf("%-36s | %10s | %10s | %10.2f MB | %12s | %10.2f µs\n",
		"C: BBolt + JSON + AES (Cold)", "-", "-", diskSizeJSONMB, "-", float64(latC1000ColdJSON.Microseconds()))
	fmt.Println("------------------------------------------------------------------------------------------------------------------------")
	fmt.Printf("Recompute 10 refs latency: A=%.2fµs, B=%.2fµs, C-Proto(Cold)=%.2fµs, C-Proto(Warm)=%.2fµs\n",
		float64(latA10.Microseconds()), float64(latB10.Microseconds()), float64(latC10ColdProto.Microseconds()), float64(latC10WarmProto.Microseconds()))
	fmt.Println("========================================================================================================================")
}

func measureRecomputeA(stateObjects map[types.NamespacedName]*corev1.ConfigMap, refs int) time.Duration {
	start := time.Now()
	snapshot := make(map[types.NamespacedName]*corev1.ConfigMap, len(stateObjects))
	for k, v := range stateObjects {
		snapshot[k] = v
	}
	for i := 0; i < refs; i++ {
		key := types.NamespacedName{Namespace: fmt.Sprintf("ns-%02d", i%20), Name: fmt.Sprintf("cm-%05d", i)}
		_ = snapshot[key]
	}
	return time.Since(start)
}

func measureRecomputeB(indexer toolscache.Indexer, stateKeys map[types.NamespacedName]string, refs int) time.Duration {
	start := time.Now()
	keysSnapshot := make(map[types.NamespacedName]string, len(stateKeys))
	for k, v := range stateKeys {
		keysSnapshot[k] = v
	}
	for i := 0; i < refs; i++ {
		key := types.NamespacedName{Namespace: fmt.Sprintf("ns-%02d", i%20), Name: fmt.Sprintf("cm-%05d", i)}
		_, _, _ = indexer.GetByKey(key.String())
	}
	return time.Since(start)
}

func measureRecomputeC(store *Store, resolver *LazyResolver, stateKeys map[types.NamespacedName]string, refs int) (cold time.Duration, warm time.Duration) {
	// Cold run (empty resolver cache)
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

	// Warm run (resolver has memoized objects)
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

func BenchmarkLazyResolver_Resolve1000(b *testing.B) {
	cipher, _ := NewEphemeralAESGCMCipher()
	store, _ := NewStore(WithCipher(cipher))
	defer store.Close()

	indexer, _ := NewBBoltIndexer(store, &corev1.ConfigMap{}, WithBucketName("ConfigMap"))
	for i := 0; i < 1000; i++ {
		cm := generateConfigMap(i)
		_ = indexer.Add(cm)
	}

	resolver := NewLazyResolver("ConfigMap", "Secret", cipher)
	snap, _ := store.NewSnapshot()
	defer snap.Close()

	// Pre-warm
	resolver.StartRecompute()
	for i := 0; i < 1000; i++ {
		key := types.NamespacedName{Namespace: fmt.Sprintf("ns-%02d", i%20), Name: fmt.Sprintf("cm-%05d", i)}
		_, _, _ = resolver.ResolveConfigMap(snap, key, fmt.Sprintf("%d", 1000+i))
	}
	resolver.FinishRecompute()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resolver.StartRecompute()
		for j := 0; j < 1000; j++ {
			key := types.NamespacedName{Namespace: fmt.Sprintf("ns-%02d", j%20), Name: fmt.Sprintf("cm-%05d", j)}
			_, _, _ = resolver.ResolveConfigMap(snap, key, fmt.Sprintf("%d", 1000+j))
		}
		resolver.FinishRecompute()
	}
}
