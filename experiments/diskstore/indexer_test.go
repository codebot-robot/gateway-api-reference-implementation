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
	"bytes"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	toolscache "k8s.io/client-go/tools/cache"
)

func TestDiskIndexer_CRUD(t *testing.T) {
	memFS := NewMemFS()
	store, err := OpenStore(WithFS(memFS), WithDir("idx_crud_store"))
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}
	defer store.Close()

	indexer, err := NewDiskIndexer(store, &corev1.ConfigMap{})
	if err != nil {
		t.Fatalf("NewDiskIndexer failed: %v", err)
	}

	cm1 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns-a",
			Name:      "cm-1",
		},
		Data: map[string]string{"key": "value1"},
	}

	// Add
	if err := indexer.Add(cm1); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// GetByKey
	obj, exists, err := indexer.GetByKey("ns-a/cm-1")
	if err != nil || !exists {
		t.Fatalf("GetByKey failed: exists=%v, err=%v", exists, err)
	}
	retrievedCM, ok := obj.(*corev1.ConfigMap)
	if !ok || retrievedCM.Data["key"] != "value1" {
		t.Fatalf("unexpected object data: %+v", retrievedCM)
	}

	// Update
	cm1Updated := cm1.DeepCopy()
	cm1Updated.Data["key"] = "value1-updated"
	if err := indexer.Update(cm1Updated); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	obj, exists, err = indexer.Get(cm1)
	if err != nil || !exists {
		t.Fatalf("Get failed: exists=%v, err=%v", exists, err)
	}
	if obj.(*corev1.ConfigMap).Data["key"] != "value1-updated" {
		t.Fatalf("update was not reflected: %+v", obj)
	}

	// List
	list := indexer.List()
	if len(list) != 1 {
		t.Fatalf("expected 1 item, got %d", len(list))
	}

	// ListKeys
	keys := indexer.ListKeys()
	if len(keys) != 1 || keys[0] != "ns-a/cm-1" {
		t.Fatalf("expected [ns-a/cm-1], got %v", keys)
	}

	// Delete
	if err := indexer.Delete(cm1); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, exists, err = indexer.GetByKey("ns-a/cm-1")
	if err != nil || exists {
		t.Fatalf("expected deleted, exists=%v, err=%v", exists, err)
	}
}

func TestDiskIndexer_SecondaryIndexing(t *testing.T) {
	memFS := NewMemFS()
	store, err := OpenStore(WithFS(memFS), WithDir("idx_sec_store"))
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}
	defer store.Close()

	customIndexers := toolscache.Indexers{
		"by-annotation": func(obj any) ([]string, error) {
			cm, ok := obj.(*corev1.ConfigMap)
			if !ok {
				return nil, fmt.Errorf("unexpected type %T", obj)
			}
			if val, ok := cm.Annotations["target-ann"]; ok {
				return []string{val}, nil
			}
			return nil, nil
		},
	}

	indexer, err := NewDiskIndexer(store, &corev1.ConfigMap{}, WithIndexers(customIndexers))
	if err != nil {
		t.Fatalf("NewDiskIndexer failed: %v", err)
	}

	cm1 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ns-1",
			Name:        "cm-1",
			Annotations: map[string]string{"target-ann": "foo"},
		},
	}
	cm2 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ns-1",
			Name:        "cm-2",
			Annotations: map[string]string{"target-ann": "foo"},
		},
	}
	cm3 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "ns-2",
			Name:        "cm-3",
			Annotations: map[string]string{"target-ann": "bar"},
		},
	}

	_ = indexer.Add(cm1)
	_ = indexer.Add(cm2)
	_ = indexer.Add(cm3)

	// ByIndex
	fooItems, err := indexer.ByIndex("by-annotation", "foo")
	if err != nil {
		t.Fatalf("ByIndex failed: %v", err)
	}
	if len(fooItems) != 2 {
		t.Fatalf("expected 2 items for foo, got %d", len(fooItems))
	}

	// IndexKeys
	fooKeys, err := indexer.IndexKeys("by-annotation", "foo")
	if err != nil {
		t.Fatalf("IndexKeys failed: %v", err)
	}
	if len(fooKeys) != 2 || fooKeys[0] != "ns-1/cm-1" || fooKeys[1] != "ns-1/cm-2" {
		t.Fatalf("unexpected keys for foo: %v", fooKeys)
	}

	// ListIndexFuncValues
	vals := indexer.ListIndexFuncValues("by-annotation")
	if len(vals) != 2 {
		t.Fatalf("expected 2 distinct values, got %v", vals)
	}

	// Update cm2 annotation from foo to bar
	cm2Updated := cm2.DeepCopy()
	cm2Updated.Annotations["target-ann"] = "bar"
	_ = indexer.Update(cm2Updated)

	fooKeys, _ = indexer.IndexKeys("by-annotation", "foo")
	if len(fooKeys) != 1 || fooKeys[0] != "ns-1/cm-1" {
		t.Fatalf("expected 1 item for foo after update, got %v", fooKeys)
	}

	barKeys, _ := indexer.IndexKeys("by-annotation", "bar")
	if len(barKeys) != 2 {
		t.Fatalf("expected 2 items for bar after update, got %v", barKeys)
	}
}

func TestDiskIndexer_WithEncryption(t *testing.T) {
	memFS := NewMemFS()
	cipher, err := NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("NewEphemeralAESGCMCipher failed: %v", err)
	}

	store, err := OpenStore(WithFS(memFS), WithCipher(cipher), WithDir("idx_crypto_store"))
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}
	defer store.Close()

	indexer, err := NewDiskIndexer(store, &corev1.Secret{}, WithIndexerCipher(cipher))
	if err != nil {
		t.Fatalf("NewDiskIndexer failed: %v", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "sensitive-secret",
		},
		Data: map[string][]byte{"token": []byte("super-secret-token-12345")},
	}

	if err := indexer.Add(secret); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Raw bytes in store must be ciphertext
	rawCiphertext, exists, err := store.Get(fmt.Sprintf("%T", secret), "kube-system/sensitive-secret")
	if err != nil || !exists {
		t.Fatalf("raw store get failed: %v", err)
	}

	if bytes.Contains(rawCiphertext, []byte("super-secret-token-12345")) {
		t.Fatalf("secret token was found in plaintext in raw store bytes!")
	}

	// Decrypt through indexer
	obj, exists, err := indexer.GetByKey("kube-system/sensitive-secret")
	if err != nil || !exists {
		t.Fatalf("GetByKey failed: %v", err)
	}
	decryptedSecret := obj.(*corev1.Secret)
	if string(decryptedSecret.Data["token"]) != "super-secret-token-12345" {
		t.Fatalf("decrypted secret mismatch: %s", string(decryptedSecret.Data["token"]))
	}
}

func TestDiskIndexer_ReplaceAndResourceVersion(t *testing.T) {
	memFS := NewMemFS()
	store, err := OpenStore(WithFS(memFS), WithDir("idx_rv_store"))
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}
	defer store.Close()

	indexer, err := NewDiskIndexer(store, &corev1.ConfigMap{})
	if err != nil {
		t.Fatalf("NewDiskIndexer failed: %v", err)
	}

	list := []any{
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "cm-1"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "cm-2"}},
	}

	if err := indexer.Replace(list, "45678"); err != nil {
		t.Fatalf("Replace failed: %v", err)
	}

	if rv := indexer.LastStoreSyncResourceVersion(); rv != "45678" {
		t.Fatalf("expected RV 45678, got %s", rv)
	}

	keys := indexer.ListKeys()
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}

	indexer.Bookmark("50000")
	if rv := indexer.LastStoreSyncResourceVersion(); rv != "50000" {
		t.Fatalf("expected Bookmark RV 50000, got %s", rv)
	}
}
