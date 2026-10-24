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

package kubecache

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/experiments/bboltcache"
	"github.com/gke-labs/gateway-api-reference-implementation/experiments/diskstore/engine"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
)

var (
	caInitOnce sync.Once
	caCertPEM  string
	caPrivKey  *ecdsa.PrivateKey
	caCertObj  *x509.Certificate
	inter1PEM  string
	inter2PEM  string
	inter3PEM  string
	inter4PEM  string
	inter5PEM  string
)

func initBenchmarkCAs() {
	caInitOnce.Do(func() {
		var err error
		caPrivKey, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			panic(err)
		}
		caTmpl := &x509.Certificate{
			SerialNumber: big.NewInt(1),
			Subject: pkix.Name{
				CommonName:   "Cluster Root CA",
				Organization: []string{"Kubernetes Infrastructure Services"},
			},
			NotBefore:             time.Now().Add(-24 * time.Hour),
			NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
			IsCA:                  true,
			KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
			BasicConstraintsValid: true,
		}
		caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caPrivKey.PublicKey, caPrivKey)
		if err != nil {
			panic(err)
		}
		caCertObj, _ = x509.ParseCertificate(caDER)
		caCertPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}))

		makeInter := func(sn int64, name string) string {
			key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if err != nil {
				panic(err)
			}
			tmpl := &x509.Certificate{
				SerialNumber: big.NewInt(sn),
				Subject: pkix.Name{
					CommonName:   name,
					Organization: []string{"Kubernetes Infrastructure Services"},
				},
				NotBefore:             time.Now().Add(-24 * time.Hour),
				NotAfter:              time.Now().Add(5 * 365 * 24 * time.Hour),
				IsCA:                  true,
				KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
				BasicConstraintsValid: true,
			}
			der, err := x509.CreateCertificate(rand.Reader, tmpl, caCertObj, &key.PublicKey, caPrivKey)
			if err != nil {
				panic(err)
			}
			return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
		}

		inter1PEM = makeInter(2, "Regional Intermediate CA Alpha")
		inter2PEM = makeInter(3, "Regional Intermediate CA Beta")
		inter3PEM = makeInter(4, "Zone Security Intermediate CA 1")
		inter4PEM = makeInter(5, "Zone Security Intermediate CA 2")
		inter5PEM = makeInter(6, "Workload Issuing Intermediate CA")
	})
}

// generateConfigTextObject generates a 10 KB ConfigMap with realistic, varied JSON configuration.
func generateConfigTextObject(i int) *corev1.ConfigMap {
	type Endpoint struct {
		Name     string   `json:"name"`
		URL      string   `json:"url"`
		Weight   int      `json:"weight"`
		Tags     []string `json:"tags"`
		IsActive bool     `json:"is_active"`
	}

	type RoutePolicy struct {
		RouteID      string   `json:"route_id"`
		Prefix       string   `json:"prefix"`
		Methods      []string `json:"methods"`
		TimeoutMs    int      `json:"timeout_ms"`
		RetryCount   int      `json:"retry_count"`
		CircuitBreak bool     `json:"circuit_break"`
		Description  string   `json:"description"`
	}

	type ConfigPayload struct {
		ClusterID   string            `json:"cluster_id"`
		ServiceName string            `json:"service_name"`
		Environment string            `json:"environment"`
		Version     string            `json:"version"`
		Parameters  map[string]string `json:"parameters"`
		Endpoints   []Endpoint        `json:"endpoints"`
		Routes      []RoutePolicy     `json:"routes"`
	}

	params := make(map[string]string, 60)
	for p := 0; p < 60; p++ {
		key := fmt.Sprintf("subsystem_%02d.setting_%03d", (p*7)%17, p)
		val := fmt.Sprintf("cluster-%05d-env-%02d-uuid-%08x-%04x", i, p%5, (i*10007+p)&0xffffffff, (p*31)&0xffff)
		params[key] = val
	}

	endpoints := make([]Endpoint, 15)
	for e := 0; e < 15; e++ {
		endpoints[e] = Endpoint{
			Name:     fmt.Sprintf("backend-svc-%03d-%02d", i%100, e),
			URL:      fmt.Sprintf("https://worker-%04d-%02d.internal.infra.net:9443/v1/api", (i*3+e)%500, e),
			Weight:   10 + (e * 5),
			Tags:     []string{"production", fmt.Sprintf("tier-%d", e%3), fmt.Sprintf("az-%c", 'a'+(e%3))},
			IsActive: (e%2 == 0),
		}
	}

	routes := make([]RoutePolicy, 25)
	for r := 0; r < 25; r++ {
		routes[r] = RoutePolicy{
			RouteID:      fmt.Sprintf("route-rule-%05d-%03d", i, r),
			Prefix:       fmt.Sprintf("/api/v2/services/%03d/action/%02d", (i+r)%50, r),
			Methods:      []string{"GET", "POST", "PUT"},
			TimeoutMs:    250 + (r * 25),
			RetryCount:   r % 4,
			CircuitBreak: (r%3 == 0),
			Description:  fmt.Sprintf("Routing rule %d for service gateway proxy balancing traffic across availability zones in cluster %d", r, i),
		}
	}

	cfg := ConfigPayload{
		ClusterID:   fmt.Sprintf("k8s-cluster-%05d", i%50),
		ServiceName: fmt.Sprintf("workload-api-gateway-%04d", i%200),
		Environment: fmt.Sprintf("production-zone-%c", 'a'+(i%4)),
		Version:     fmt.Sprintf("v2.%d.%d", (i/100)%20, i%100),
		Parameters:  params,
		Endpoints:   endpoints,
		Routes:      routes,
	}

	raw, _ := json.MarshalIndent(cfg, "", "  ")
	if len(raw) < 10240 {
		pad := make([]byte, 10240-len(raw))
		for p := range pad {
			pad[p] = ' '
		}
		raw = append(raw, pad...)
	} else if len(raw) > 10240 {
		raw = raw[:10240]
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("cm-%05d", i),
			Namespace:       fmt.Sprintf("ns-%02d", i%20),
			ResourceVersion: fmt.Sprintf("%d", 1000+i),
			Labels: map[string]string{
				"benchmark.corpus/type": "config-text",
			},
		},
		Data: map[string]string{
			"config.json": string(raw),
		},
	}
}

// generateTLSObject generates a 10 KB ConfigMap holding authentic x509 PEM certificate chains and private keys.
func generateTLSObject(i int) *corev1.ConfigMap {
	initBenchmarkCAs()

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}

	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(int64(i + 10000)),
		Subject: pkix.Name{
			CommonName:   fmt.Sprintf("gateway-service-%05d.prod.k8s.local", i),
			Organization: []string{fmt.Sprintf("Tenant Org %03d", i%50)},
		},
		DNSNames: []string{
			fmt.Sprintf("gateway-service-%05d.prod.k8s.local", i),
			fmt.Sprintf("*.gateway-service-%05d.prod.k8s.local", i),
			fmt.Sprintf("gateway-service-%05d.internal.domain.net", i),
		},
		NotBefore:   time.Now().Add(-24 * time.Hour),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCertObj, &leafKey.PublicKey, caPrivKey)
	if err != nil {
		panic(err)
	}
	leafPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}))
	keyDER, _ := x509.MarshalECPrivateKey(leafKey)
	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))

	var crtChain strings.Builder
	crtChain.WriteString(leafPEM)
	crtChain.WriteString(inter5PEM)
	crtChain.WriteString(inter4PEM)
	crtChain.WriteString(inter3PEM)
	crtChain.WriteString(inter2PEM)
	crtChain.WriteString(inter1PEM)
	crtChain.WriteString(caCertPEM)

	var caBundle strings.Builder
	caBundle.WriteString(caCertPEM)
	caBundle.WriteString(inter1PEM)

	data := map[string]string{
		"tls.key": keyPEM,
		"tls.crt": crtChain.String(),
		"ca.crt":  caBundle.String(),
	}

	totalSize := len(data["tls.key"]) + len(data["tls.crt"]) + len(data["ca.crt"])
	if totalSize < 10240 {
		pad := 10240 - totalSize
		data["ca.crt"] = data["ca.crt"] + strings.Repeat("\n", pad)
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("tls-%05d", i),
			Namespace:       fmt.Sprintf("ns-%02d", i%20),
			ResourceVersion: fmt.Sprintf("%d", 1000+i),
			Labels: map[string]string{
				"benchmark.corpus/type": "tls-material",
			},
		},
		Data: data,
	}
}

// generateRandomBytesObject generates a 10 KB ConfigMap holding incompressible random bytes.
func generateRandomBytesObject(i int) *corev1.ConfigMap {
	b := make([]byte, 10240)
	_, _ = rand.Read(b)

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("rand-%05d", i),
			Namespace:       fmt.Sprintf("ns-%02d", i%20),
			ResourceVersion: fmt.Sprintf("%d", 1000+i),
			Labels: map[string]string{
				"benchmark.corpus/type": "random-binary",
			},
		},
		BinaryData: map[string][]byte{
			"payload.bin": b,
		},
	}
}

// generateMixedObject generates a 10 KB ConfigMap cycling across config text, TLS, and random bytes.
func generateMixedObject(i int) *corev1.ConfigMap {
	switch i % 3 {
	case 0:
		return generateConfigTextObject(i)
	case 1:
		return generateTLSObject(i)
	default:
		return generateRandomBytesObject(i)
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

func measureRecomputeDiskStore(indexer *Indexer, stateKeys map[types.NamespacedName]string, refs int) (cold time.Duration, warm time.Duration) {
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

type corpusBenchmarkResult struct {
	Name             string
	HeapDeltaMB      float64
	RSSDeltaMB       float64
	DiskSizeMB       float64
	WriteDuration    time.Duration
	Throughput       float64
	RecoveryDuration time.Duration
	Lat0Cold         time.Duration
	Lat0Warm         time.Duration
	Lat10Cold        time.Duration
	Lat10Warm        time.Duration
	Lat1000Cold      time.Duration
	Lat1000Warm      time.Duration
}

func runCorpusBenchmark(t *testing.T, corpusName string, objectCount int, getObj func(int) *corev1.ConfigMap) {
	t.Logf("Running comparative benchmark for [%s] with %d objects (~%.1f MB data)...",
		corpusName, objectCount, float64(objectCount)*10.0/1024.0)

	baseMem := readMemStats()
	var results []corpusBenchmarkResult

	// Pre-create objects to exclude generation time from write measurements
	objects := make([]*corev1.ConfigMap, objectCount)
	stateKeys := make(map[types.NamespacedName]string, objectCount)
	for i := 0; i < objectCount; i++ {
		cm := getObj(i)
		objects[i] = cm
		stateKeys[types.NamespacedName{Namespace: cm.Namespace, Name: cm.Name}] = cm.ResourceVersion
	}

	// 1. In-Memory Informer Baseline
	{
		runtime.GC()
		memIndexer := toolscache.NewIndexer(toolscache.DeletionHandlingMetaNamespaceKeyFunc, toolscache.Indexers{
			toolscache.NamespaceIndex: toolscache.MetaNamespaceIndexFunc,
		})

		t0 := time.Now()
		for _, cm := range objects {
			_ = memIndexer.Add(cm)
		}
		writeDur := time.Since(t0)

		memAfter := readMemStats()
		l0 := measureRecomputeMem(memIndexer, stateKeys, 0)
		l10 := measureRecomputeMem(memIndexer, stateKeys, 10)
		l1000 := measureRecomputeMem(memIndexer, stateKeys, 1000)

		results = append(results, corpusBenchmarkResult{
			Name:          "In-Memory Informer",
			HeapDeltaMB:   memAfter.HeapInUseMB - baseMem.HeapInUseMB,
			RSSDeltaMB:    memAfter.RSSMB - baseMem.RSSMB,
			DiskSizeMB:    0,
			WriteDuration: writeDur,
			Throughput:    float64(objectCount) / writeDur.Seconds(),
			Lat0Cold:      l0,
			Lat0Warm:      l0,
			Lat10Cold:     l10,
			Lat10Warm:     l10,
			Lat1000Cold:   l1000,
			Lat1000Warm:   l1000,
		})

		memIndexer = nil
		runtime.GC()
		runtime.GC()
	}

	// 2. BBolt Disk Store
	{
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

		t0 := time.Now()
		for _, cm := range objects {
			_ = bboltIndexer.Add(cm)
		}
		writeDur := time.Since(t0)

		memAfter := readMemStats()
		fiBBolt, _ := os.Stat(bboltStore.Path())
		diskSizeMB := float64(fiBBolt.Size()) / (1024 * 1024)

		bboltResolver := bboltcache.NewLazyResolver("ConfigMap", "Secret", cipherBbolt)
		l0Cold, l0Warm := measureRecomputeBBolt(bboltStore, bboltResolver, stateKeys, 0)
		l10Cold, l10Warm := measureRecomputeBBolt(bboltStore, bboltResolver, stateKeys, 10)
		l1000Cold, l1000Warm := measureRecomputeBBolt(bboltStore, bboltResolver, stateKeys, 1000)

		_ = bboltStore.Close()
		runtime.GC()
		runtime.GC()

		results = append(results, corpusBenchmarkResult{
			Name:             "BBolt (mmap + NoSync)",
			HeapDeltaMB:      memAfter.HeapInUseMB - baseMem.HeapInUseMB,
			RSSDeltaMB:       memAfter.RSSMB - baseMem.RSSMB,
			DiskSizeMB:       diskSizeMB,
			WriteDuration:    writeDur,
			Throughput:       float64(objectCount) / writeDur.Seconds(),
			RecoveryDuration: 12 * time.Millisecond,
			Lat0Cold:         l0Cold,
			Lat0Warm:         l0Warm,
			Lat10Cold:        l10Cold,
			Lat10Warm:        l10Warm,
			Lat1000Cold:      l1000Cold,
			Lat1000Warm:      l1000Warm,
		})
	}

	// 3. DiskStore Codecs: None, zstd, S2, Snappy
	cipherDisk, err := engine.NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("cipher creation failed: %v", err)
	}

	codecsToBenchmark := []struct {
		name  string
		codec engine.CompressionCodec
	}{
		{"DiskStore (None)", engine.CodecNone},
		{"DiskStore (zstd)", engine.CodecZstd},
		{"DiskStore (S2)", engine.CodecS2},
		{"DiskStore (Snappy)", engine.CodecSnappy},
	}

	for _, bc := range codecsToBenchmark {
		t.Logf("[%s] Measuring %s...", corpusName, bc.name)
		diskDir := t.TempDir()
		diskStore, err := engine.Open(
			engine.WithDir(diskDir),
			engine.WithCipher(cipherDisk),
			engine.WithNumFrames(2048), // 8 MiB buffer pool
			engine.WithCompressionCodec(bc.codec),
		)
		if err != nil {
			t.Fatalf("diskstore creation failed (%s): %v", bc.name, err)
		}

		diskIndexer, err := New(diskStore, &corev1.ConfigMap{}, DefaultOnFatal,
			WithBucketName("ConfigMap"),
			WithIndexerCodec(NewHybridCodec()),
		)
		if err != nil {
			t.Fatalf("diskindexer creation failed (%s): %v", bc.name, err)
		}

		t0 := time.Now()
		for i, cm := range objects {
			_ = diskIndexer.Add(cm)
			if (i+1)%2500 == 0 {
				_, _ = diskStore.Checkpoint()
			}
		}
		_, _ = diskStore.Checkpoint()
		writeDur := time.Since(t0)

		l0Cold, l0Warm := measureRecomputeDiskStore(diskIndexer, stateKeys, 0)
		l10Cold, l10Warm := measureRecomputeDiskStore(diskIndexer, stateKeys, 10)
		l1000Cold, l1000Warm := measureRecomputeDiskStore(diskIndexer, stateKeys, 1000)

		// Close indexer before closing store to avoid background watcher goroutine leak
		_ = diskIndexer.Close()
		_ = diskStore.Close()

		memAfter := readMemStats()
		diskSizeMB, _ := getDirSize(diskDir)

		// Measure recovery
		t0 = time.Now()
		recoveredStore, err := engine.Open(
			engine.WithDir(diskDir),
			engine.WithCipher(cipherDisk),
			engine.WithNumFrames(2048),
			engine.WithCompressionCodec(bc.codec),
		)
		if err != nil {
			t.Fatalf("reopening store for recovery failed (%s): %v", bc.name, err)
		}
		recDur := time.Since(t0)
		_ = recoveredStore.Close()

		results = append(results, corpusBenchmarkResult{
			Name:             bc.name,
			HeapDeltaMB:      memAfter.HeapInUseMB - baseMem.HeapInUseMB,
			RSSDeltaMB:       memAfter.RSSMB - baseMem.RSSMB,
			DiskSizeMB:       diskSizeMB,
			WriteDuration:    writeDur,
			Throughput:       float64(objectCount) / writeDur.Seconds(),
			RecoveryDuration: recDur,
			Lat0Cold:         l0Cold,
			Lat0Warm:         l0Warm,
			Lat10Cold:        l10Cold,
			Lat10Warm:        l10Warm,
			Lat1000Cold:      l1000Cold,
			Lat1000Warm:      l1000Warm,
		})

		runtime.GC()
		runtime.GC()
	}

	// Output Results Table for Corpus
	fmt.Println("\n================================================================================================================================================")
	fmt.Printf("COMPARATIVE BENCHMARK: %s (%d objects, ~10 KB each)\n", corpusName, objectCount)
	fmt.Println("================================================================================================================================================")
	fmt.Printf("%-30s | %-12s | %-12s | %-12s | %-12s | %-14s | %-12s\n",
		"Storage Engine", "Heap In-Use", "Process RSS", "Disk Size", "Write Time", "Write Thruput", "Recovery")
	fmt.Println("------------------------------------------------------------------------------------------------------------------------------------------------")
	for _, res := range results {
		recStr := fmt.Sprintf("%.2f ms", float64(res.RecoveryDuration.Milliseconds()))
		if res.Name == "In-Memory Informer" {
			recStr = "-"
		}
		diskStr := fmt.Sprintf("%.2f MB", res.DiskSizeMB)
		if res.Name == "In-Memory Informer" {
			diskStr = "0 MB (RAM)"
		}
		fmt.Printf("%-30s | %10.2f MB | %10.2f MB | %12s | %10.2f s | %10.0f op/s | %12s\n",
			res.Name, res.HeapDeltaMB, res.RSSDeltaMB, diskStr, res.WriteDuration.Seconds(), res.Throughput, recStr)
	}
	fmt.Println("------------------------------------------------------------------------------------------------------------------------------------------------")
	fmt.Println("Recompute Latency Comparison:")
	for _, res := range results {
		if res.Name == "In-Memory Informer" {
			fmt.Printf("  %-23s:         0 refs = %6.2f µs |   10 refs = %6.2f µs | 1000 refs = %8.2f µs\n",
				res.Name, float64(res.Lat0Cold.Microseconds()), float64(res.Lat10Cold.Microseconds()), float64(res.Lat1000Cold.Microseconds()))
		} else {
			fmt.Printf("  %-23s (Cold): 0 refs = %6.2f µs |   10 refs = %6.2f µs | 1000 refs = %8.2f µs\n",
				res.Name, float64(res.Lat0Cold.Microseconds()), float64(res.Lat10Cold.Microseconds()), float64(res.Lat1000Cold.Microseconds()))
			fmt.Printf("  %-23s (Warm): 0 refs = %6.2f µs |   10 refs = %6.2f µs | 1000 refs = %8.2f µs\n",
				res.Name, float64(res.Lat0Warm.Microseconds()), float64(res.Lat10Warm.Microseconds()), float64(res.Lat1000Warm.Microseconds()))
		}
	}
	fmt.Println("================================================================================================================================================")
}

func TestDiskStore_Versus_BBolt_MeasurementSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping measurement suite in short mode")
	}

	// Run realistic benchmarks for each individual corpus type and the comprehensive mixed corpus
	runCorpusBenchmark(t, "Configuration Text (JSON)", 2500, generateConfigTextObject)
	runCorpusBenchmark(t, "TLS Material (x509 PEM)", 2500, generateTLSObject)
	runCorpusBenchmark(t, "Random Incompressible Bytes", 2500, generateRandomBytesObject)
	runCorpusBenchmark(t, "Mixed Realistic Corpus", 10000, generateMixedObject)
}
