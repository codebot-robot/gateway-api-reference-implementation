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

// generateConfigMaps creates n ConfigMaps with approximately 10 KB of payload each.
func generateConfigMaps(n int) []*corev1.ConfigMap {
	cms := make([]*corev1.ConfigMap, n)
	// 10 KB string payload
	payload := strings.Repeat("abcdef0123456789", 640) // 10,240 bytes

	for i := 0; i < n; i++ {
		cms[i] = &corev1.ConfigMap{
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
	return cms
}

func readHeapMB() float64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / (1024 * 1024)
}

// TestMeasurementSuite runs the 3 implementation spikes and outputs empirical results.
func TestMeasurementSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping measurement suite in short mode")
	}

	const objectCount = 10000
	t.Logf("Generating %d ConfigMaps (10 KB each)...", objectCount)
	cms := generateConfigMaps(objectCount)

	// Measure baseline: Raw generated slice
	baseHeap := readHeapMB()

	// -------------------------------------------------------------
	// 1. Implementation A: In-Memory Informer + Full Objects in State (Status Quo)
	// -------------------------------------------------------------
	t.Log("Measuring Implementation A: In-Memory Cache + Full Objects in State...")
	startHeapA := readHeapMB()

	memIndexer := toolscache.NewIndexer(toolscache.DeletionHandlingMetaNamespaceKeyFunc, toolscache.Indexers{
		toolscache.NamespaceIndex: toolscache.MetaNamespaceIndexFunc,
	})
	stateObjects := make(map[types.NamespacedName]*corev1.ConfigMap, objectCount)

	for _, cm := range cms {
		_ = memIndexer.Add(cm)
		key := types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}
		stateObjects[key] = cm.DeepCopy() // Status quo copies full object into State
	}
	heapAfterPopulateA := readHeapMB()
	heapDeltaA := heapAfterPopulateA - startHeapA

	// Recompute latency with 0, 10, 1000 referenced
	latA0 := measureRecomputeA(stateObjects, 0)
	latA10 := measureRecomputeA(stateObjects, 10)
	latA1000 := measureRecomputeA(stateObjects, 1000)

	// Clean up A
	memIndexer = nil
	stateObjects = nil
	runtime.GC()

	// -------------------------------------------------------------
	// 2. Implementation B: In-Memory Informer + Keys-only in State
	// -------------------------------------------------------------
	t.Log("Measuring Implementation B: In-Memory Cache + Keys-only in State...")
	startHeapB := readHeapMB()

	memIndexerB := toolscache.NewIndexer(toolscache.DeletionHandlingMetaNamespaceKeyFunc, toolscache.Indexers{
		toolscache.NamespaceIndex: toolscache.MetaNamespaceIndexFunc,
	})
	stateKeysB := make(map[types.NamespacedName]string, objectCount)

	for _, cm := range cms {
		_ = memIndexerB.Add(cm)
		key := types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}
		stateKeysB[key] = cm.ResourceVersion
	}
	heapAfterPopulateB := readHeapMB()
	heapDeltaB := heapAfterPopulateB - startHeapB

	latB0 := measureRecomputeB(memIndexerB, stateKeysB, 0)
	latB10 := measureRecomputeB(memIndexerB, stateKeysB, 10)
	latB1000 := measureRecomputeB(memIndexerB, stateKeysB, 1000)

	memIndexerB = nil
	stateKeysB = nil
	runtime.GC()

	// -------------------------------------------------------------
	// 3. Implementation C: BBolt Disk-Backed Store + Encrypted + Keys-only in State + Lazy Snapshot Resolver
	// -------------------------------------------------------------
	t.Log("Measuring Implementation C: BBolt Store + AES-GCM + Keys-only in State...")
	cipher, err := NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("cipher creation failed: %v", err)
	}

	startHeapC := readHeapMB()
	store, err := NewStore(WithCipher(cipher))
	if err != nil {
		t.Fatalf("store creation failed: %v", err)
	}
	defer store.Close()

	bboltIndexer, err := NewBBoltIndexer(store, &corev1.ConfigMap{},
		WithBucketName("ConfigMap"),
		WithIndexerCipher(cipher),
		WithIndexers(toolscache.Indexers{
			toolscache.NamespaceIndex: toolscache.MetaNamespaceIndexFunc,
		}),
	)
	if err != nil {
		t.Fatalf("indexer creation failed: %v", err)
	}

	stateKeysC := make(map[types.NamespacedName]string, objectCount)
	for _, cm := range cms {
		_ = bboltIndexer.Add(cm)
		key := types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}
		stateKeysC[key] = cm.ResourceVersion
	}
	heapAfterPopulateC := readHeapMB()
	heapDeltaC := heapAfterPopulateC - startHeapC

	// Check disk file size
	fi, _ := os.Stat(store.Path())
	diskSizeBytes := fi.Size()
	diskSizeMB := float64(diskSizeBytes) / (1024 * 1024)

	resolver := NewLazyResolver("ConfigMap", "Secret", cipher)

	// Measure recompute latency (cold and warm memoized)
	latC0Cold, latC0Warm := measureRecomputeC(store, resolver, stateKeysC, 0)
	latC10Cold, latC10Warm := measureRecomputeC(store, resolver, stateKeysC, 10)
	latC1000Cold, latC1000Warm := measureRecomputeC(store, resolver, stateKeysC, 1000)

	// Print Summary Report Table
	fmt.Println("\n==================================================================================================")
	fmt.Printf("EMERGENT EMPIRICAL MEASUREMENTS: N = %d ConfigMaps (10 KB each)\n", objectCount)
	fmt.Println("==================================================================================================")
	fmt.Printf("Baseline Heap (Object Gen): %.2f MB\n", baseHeap)
	fmt.Println("--------------------------------------------------------------------------------------------------")
	fmt.Printf("%-35s | %-12s | %-12s | %-12s | %-12s\n", "Implementation Architecture", "Heap In-Use", "Disk Size", "Recomp (0)", "Recomp (1000)")
	fmt.Println("--------------------------------------------------------------------------------------------------")
	fmt.Printf("%-35s | %10.2f MB | %10s | %10.2f µs | %10.2f µs\n", "A: In-Memory Informer + Full State", heapDeltaA, "0 MB (RAM)", float64(latA0.Microseconds()), float64(latA1000.Microseconds()))
	fmt.Printf("%-35s | %10.2f MB | %10s | %10.2f µs | %10.2f µs\n", "B: In-Memory Informer + Keys State", heapDeltaB, "0 MB (RAM)", float64(latB0.Microseconds()), float64(latB1000.Microseconds()))
	fmt.Printf("%-35s | %10.2f MB | %10.2f MB | %10.2f µs | %10.2f µs\n", "C: BBolt Disk + AES-GCM (Cold)", heapDeltaC, diskSizeMB, float64(latC0Cold.Microseconds()), float64(latC1000Cold.Microseconds()))
	fmt.Printf("%-35s | %10.2f MB | %10.2f MB | %10.2f µs | %10.2f µs\n", "C: BBolt Disk + AES-GCM (Warm)", heapDeltaC, diskSizeMB, float64(latC0Warm.Microseconds()), float64(latC1000Warm.Microseconds()))
	fmt.Println("--------------------------------------------------------------------------------------------------")
	fmt.Printf("Recompute 10 refs latency: A=%.2fµs, B=%.2fµs, C(Cold)=%.2fµs, C(Warm)=%.2fµs\n",
		float64(latA10.Microseconds()), float64(latB10.Microseconds()), float64(latC10Cold.Microseconds()), float64(latC10Warm.Microseconds()))
	fmt.Println("==================================================================================================")
}

func measureRecomputeA(stateObjects map[types.NamespacedName]*corev1.ConfigMap, refs int) time.Duration {
	// Status quo: snapshots copy map, and referenced objects are accessed
	start := time.Now()
	// Deep copy map as State.buildModelInputsLocked does
	snapshot := make(map[types.NamespacedName]*corev1.ConfigMap, len(stateObjects))
	for k, v := range stateObjects {
		snapshot[k] = v
	}
	// Access referenced objects
	for i := 0; i < refs; i++ {
		key := types.NamespacedName{Namespace: fmt.Sprintf("ns-%02d", i%20), Name: fmt.Sprintf("cm-%05d", i)}
		_ = snapshot[key]
	}
	return time.Since(start)
}

func measureRecomputeB(indexer toolscache.Indexer, stateKeys map[types.NamespacedName]string, refs int) time.Duration {
	start := time.Now()
	// State only copies keys map
	keysSnapshot := make(map[types.NamespacedName]string, len(stateKeys))
	for k, v := range stateKeys {
		keysSnapshot[k] = v
	}
	// Access referenced objects from in-memory cache
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
	snapCold, _ := store.NewSnapshot()
	for i := 0; i < refs; i++ {
		key := types.NamespacedName{Namespace: fmt.Sprintf("ns-%02d", i%20), Name: fmt.Sprintf("cm-%05d", i)}
		rv := stateKeys[key]
		_, _, _ = resolver.ResolveConfigMap(snapCold, key, rv)
	}
	_ = snapCold.Close()
	cold = time.Since(startCold)

	// Warm run (resolver has memoized objects)
	startWarm := time.Now()
	snapWarm, _ := store.NewSnapshot()
	for i := 0; i < refs; i++ {
		key := types.NamespacedName{Namespace: fmt.Sprintf("ns-%02d", i%20), Name: fmt.Sprintf("cm-%05d", i)}
		rv := stateKeys[key]
		_, _, _ = resolver.ResolveConfigMap(snapWarm, key, rv)
	}
	_ = snapWarm.Close()
	warm = time.Since(startWarm)

	return cold, warm
}

func BenchmarkLazyResolver_Resolve1000(b *testing.B) {
	cipher, _ := NewEphemeralAESGCMCipher()
	store, _ := NewStore(WithCipher(cipher))
	defer store.Close()

	indexer, _ := NewBBoltIndexer(store, &corev1.ConfigMap{}, WithBucketName("ConfigMap"))
	cms := generateConfigMaps(1000)
	for _, cm := range cms {
		_ = indexer.Add(cm)
	}

	resolver := NewLazyResolver("ConfigMap", "Secret", cipher)
	snap, _ := store.NewSnapshot()
	defer snap.Close()

	// Pre-warm
	for _, cm := range cms {
		key := types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}
		_, _, _ = resolver.ResolveConfigMap(snap, key, cm.ResourceVersion)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, cm := range cms {
			key := types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}
			_, _, _ = resolver.ResolveConfigMap(snap, key, cm.ResourceVersion)
		}
	}
}
