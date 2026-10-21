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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestSnapshot_PointInTimeIsolation(t *testing.T) {
	cipher, err := NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("cipher err: %v", err)
	}

	store, err := NewStore(WithCipher(cipher))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	cmIndexer, err := NewBBoltIndexer(store, &corev1.ConfigMap{}, WithBucketName("ConfigMap"))
	if err != nil {
		t.Fatalf("cm indexer err: %v", err)
	}

	// Insert v1
	cm1 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cm",
			Namespace:       "default",
			ResourceVersion: "1",
		},
		Data: map[string]string{"version": "v1"},
	}
	if err := cmIndexer.Add(cm1); err != nil {
		t.Fatalf("add err: %v", err)
	}

	// Open read snapshot
	snap, err := store.NewSnapshot()
	if err != nil {
		t.Fatalf("snapshot err: %v", err)
	}
	defer snap.Close()

	// Update object in store to v2 while snapshot is open
	cm2 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cm",
			Namespace:       "default",
			ResourceVersion: "2",
		},
		Data: map[string]string{"version": "v2"},
	}
	if err := cmIndexer.Update(cm2); err != nil {
		t.Fatalf("update err: %v", err)
	}

	// Read from snapshot: MUST still see v1!
	snapCM, exists, err := snap.GetConfigMap("ConfigMap", "default/cm", cipher)
	if err != nil || !exists {
		t.Fatalf("snap read err: %v, exists=%v", err, exists)
	}
	if snapCM.Data["version"] != "v1" {
		t.Fatalf("snapshot isolation violated: expected v1, got %v", snapCM.Data["version"])
	}

	// Indexer outside snapshot sees v2
	curObj, exists, err := cmIndexer.GetByKey("default/cm")
	if err != nil || !exists || curObj.(*corev1.ConfigMap).Data["version"] != "v2" {
		t.Fatalf("indexer does not see latest update")
	}
}

func TestLazyResolver_MemoizationAndEviction(t *testing.T) {
	cipher, err := NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("cipher err: %v", err)
	}

	store, err := NewStore(WithCipher(cipher))
	if err != nil {
		t.Fatalf("store err: %v", err)
	}
	defer store.Close()

	cmIndexer, err := NewBBoltIndexer(store, &corev1.ConfigMap{}, WithBucketName("ConfigMap"))
	if err != nil {
		t.Fatalf("indexer err: %v", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "my-ca",
			Namespace:       "default",
			ResourceVersion: "100",
		},
		Data: map[string]string{"ca.crt": "certificate-data"},
	}
	_ = cmIndexer.Add(cm)

	resolver := NewLazyResolver("ConfigMap", "Secret", cipher)
	key := types.NamespacedName{Namespace: "default", Name: "my-ca"}

	snap1, err := store.NewSnapshot()
	if err != nil {
		t.Fatalf("snap err: %v", err)
	}
	defer snap1.Close()

	// Initial resolution (cache miss)
	res1, exists, err := resolver.ResolveConfigMap(snap1, key, "100")
	if err != nil || !exists {
		t.Fatalf("resolve failed: %v", err)
	}
	if resolver.Len() != 1 {
		t.Fatalf("expected 1 memoized item, got %d", resolver.Len())
	}

	// Second resolution with same RV (cache hit: should return exact same pointer)
	res2, exists, err := resolver.ResolveConfigMap(snap1, key, "100")
	if err != nil || !exists {
		t.Fatalf("resolve failed: %v", err)
	}
	if res1 != res2 {
		t.Fatal("expected memoized pointer identity match on cache hit")
	}

	// Update RV to 101 in store
	cmUpdated := cm.DeepCopy()
	cmUpdated.ResourceVersion = "101"
	cmUpdated.Data["ca.crt"] = "new-cert"
	_ = cmIndexer.Update(cmUpdated)

	snap2, err := store.NewSnapshot()
	if err != nil {
		t.Fatalf("snap2 err: %v", err)
	}
	defer snap2.Close()

	// Resolve with RV 101 (cache miss for 101, fetches new object)
	res3, exists, err := resolver.ResolveConfigMap(snap2, key, "101")
	if err != nil || !exists {
		t.Fatalf("resolve failed: %v", err)
	}
	if res3.Data["ca.crt"] != "new-cert" {
		t.Fatalf("expected new cert, got %v", res3.Data["ca.crt"])
	}
	if res3 == res1 {
		t.Fatal("expected different pointer after RV update")
	}

	// Evict stale
	activeKeys := map[string]string{
		memoKey("ConfigMap", key, "101"): "101",
	}
	resolver.EvictStale(activeKeys)
	if resolver.Len() != 1 {
		t.Fatalf("expected 1 item after stale eviction, got %d", resolver.Len())
	}
}
