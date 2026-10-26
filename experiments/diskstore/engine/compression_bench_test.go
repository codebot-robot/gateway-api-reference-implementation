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

package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkPut(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"100B", 100},
		{"500B", 500},
		{"1KB", 1024},
		{"4KB", 4096},
	}

	for _, s := range sizes {
		b.Run("Compressible/"+s.name, func(b *testing.B) {
			payload := makeRepetitivePayload(s.size)
			runBenchmarkPut(b, payload)
		})
		b.Run("Incompressible/"+s.name, func(b *testing.B) {
			payload := makeRandomPayload(s.size)
			runBenchmarkPut(b, payload)
		})
	}
}

func runBenchmarkPut(b *testing.B, payload []byte) {
	dir := b.TempDir()
	store, err := Open(WithDir(dir), WithNumFrames(4096))
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("k-%08d", i)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := store.Put("bench", keys[i], payload, "1"); err != nil {
			b.Fatalf("Put failed at %d: %v", i, err)
		}
	}
}

// generateRealisticK8sObjectJSON generates a realistic JSON Kubernetes object (e.g. Pod/Service)
// with a size mix mostly between 1 KB and 4 KB.
func generateRealisticK8sObjectJSON(i int) []byte {
	// Target sizes: cycle across 1 KB, 1.5 KB, 2.2 KB, 3.1 KB, 3.9 KB
	targetSizes := []int{1050, 1500, 2200, 3100, 3900}
	targetSize := targetSizes[i%len(targetSizes)]

	type EnvVar struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	type Container struct {
		Name  string   `json:"name"`
		Image string   `json:"image"`
		Env   []EnvVar `json:"env"`
	}
	type PodSpec struct {
		Containers    []Container       `json:"containers"`
		NodeSelector  map[string]string `json:"nodeSelector"`
		RestartPolicy string            `json:"restartPolicy"`
	}
	type PodObject struct {
		APIVersion string            `json:"apiVersion"`
		Kind       string            `json:"kind"`
		Metadata   map[string]any    `json:"metadata"`
		Spec       PodSpec           `json:"spec"`
		Status     map[string]string `json:"status"`
	}

	numEnv := (targetSize - 500) / 75
	if numEnv < 2 {
		numEnv = 2
	}
	envs := make([]EnvVar, numEnv)
	for e := 0; e < numEnv; e++ {
		envs[e] = EnvVar{
			Name:  fmt.Sprintf("ENV_VAR_SETTING_%03d", e),
			Value: fmt.Sprintf("val-cluster-zone-%02d-pod-%05d-seq-%04d", e%10, i, e),
		}
	}

	labels := map[string]string{
		"app.kubernetes.io/name":       fmt.Sprintf("workload-svc-%02d", i%30),
		"app.kubernetes.io/instance":   fmt.Sprintf("instance-%05d", i),
		"app.kubernetes.io/version":    fmt.Sprintf("v1.%d.%d", (i/50)%10, i%50),
		"app.kubernetes.io/managed-by": "helm",
	}

	pod := PodObject{
		APIVersion: "v1",
		Kind:       "Pod",
		Metadata: map[string]any{
			"name":            fmt.Sprintf("pod-%05d", i),
			"namespace":       fmt.Sprintf("namespace-%02d", i%15),
			"resourceVersion": fmt.Sprintf("%d", 50000+i),
			"uid":             fmt.Sprintf("12345678-1234-1234-1234-%012x", i),
			"labels":          labels,
		},
		Spec: PodSpec{
			Containers: []Container{
				{
					Name:  "main-workload",
					Image: fmt.Sprintf("gcr.io/company-registry/workload-service:%d.0", (i%20)+1),
					Env:   envs,
				},
			},
			NodeSelector: map[string]string{
				"kubernetes.io/os":   "linux",
				"kubernetes.io/arch": "amd64",
			},
			RestartPolicy: "Always",
		},
		Status: map[string]string{
			"phase": "Running",
			"hostIP": fmt.Sprintf("10.240.%d.%d", (i/250)%250, i%250),
			"podIP":  fmt.Sprintf("10.0.%d.%d", (i/250)%250, i%250),
		},
	}

	raw, _ := json.Marshal(pod)
	return raw
}

func TestRealisticCorpusHeapSize(t *testing.T) {
	const objectCount = 10000
	dir := t.TempDir()

	store, err := Open(WithDir(dir), WithNumFrames(4096), WithCompressionCodec(CodecZstd))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	var totalRawBytes int64
	start := time.Now()
	for i := 0; i < objectCount; i++ {
		payload := generateRealisticK8sObjectJSON(i)
		totalRawBytes += int64(len(payload))
		key := fmt.Sprintf("k8s-pod-%05d", i)
		if err := store.Put("pods", key, payload, fmt.Sprintf("%d", i)); err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}
	}
	writeDur := time.Since(start)

	if _, err := store.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}
	if err := store.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	heapStat, err := os.Stat(filepath.Join(dir, "heap.db"))
	if err != nil {
		t.Fatalf("Stat heap.db failed: %v", err)
	}

	var walTotalBytes int64
	walFiles, _ := filepath.Glob(filepath.Join(dir, "wal", "*.wal"))
	for _, f := range walFiles {
		st, _ := os.Stat(f)
		walTotalBytes += st.Size()
	}

	t.Logf("Realistic Corpus: %d objects", objectCount)
	t.Logf("Total raw payload bytes: %.2f MB (avg %.0f B/obj)", float64(totalRawBytes)/(1024*1024), float64(totalRawBytes)/float64(objectCount))
	t.Logf("Heap file size: %.2f MB (%d bytes)", float64(heapStat.Size())/(1024*1024), heapStat.Size())
	t.Logf("WAL size: %.2f MB", float64(walTotalBytes)/(1024*1024))
	t.Logf("Write duration: %v (%.0f op/s, %.1f ns/op)", writeDur, float64(objectCount)/writeDur.Seconds(), float64(writeDur.Nanoseconds())/float64(objectCount))
}
