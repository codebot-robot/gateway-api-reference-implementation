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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	toolscache "k8s.io/client-go/tools/cache"
)

func TestBBoltIndexer_CRUD(t *testing.T) {
	store, err := NewStore()
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	indexer, err := NewBBoltIndexer(store, &corev1.ConfigMap{})
	if err != nil {
		t.Fatalf("failed to create indexer: %v", err)
	}

	cm1 := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cm-1",
			Namespace:       "ns-a",
			ResourceVersion: "100",
		},
		Data: map[string]string{"foo": "bar"},
	}

	// Add
	if err := indexer.Add(cm1); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Get
	item, exists, err := indexer.Get(cm1)
	if err != nil || !exists {
		t.Fatalf("Get failed: exists=%v, err=%v", exists, err)
	}
	resCM, ok := item.(*corev1.ConfigMap)
	if !ok || resCM.Data["foo"] != "bar" {
		t.Fatalf("unexpected object returned: %v", item)
	}

	// GetByKey
	item, exists, err = indexer.GetByKey("ns-a/cm-1")
	if err != nil || !exists {
		t.Fatalf("GetByKey failed: exists=%v, err=%v", exists, err)
	}

	// Update
	cm1Updated := cm1.DeepCopy()
	cm1Updated.Data["foo"] = "baz"
	cm1Updated.ResourceVersion = "101"
	if err := indexer.Update(cm1Updated); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	item, exists, err = indexer.GetByKey("ns-a/cm-1")
	if err != nil || !exists || item.(*corev1.ConfigMap).Data["foo"] != "baz" {
		t.Fatalf("Update did not persist: %v", item)
	}

	// ListKeys
	keys := indexer.ListKeys()
	if len(keys) != 1 || keys[0] != "ns-a/cm-1" {
		t.Fatalf("ListKeys mismatch: %v", keys)
	}

	// List
	list := indexer.List()
	if len(list) != 1 || list[0].(*corev1.ConfigMap).Name != "cm-1" {
		t.Fatalf("List mismatch: %v", list)
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

func TestBBoltIndexer_SecondaryIndexing(t *testing.T) {
	store, err := NewStore()
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Add namespace indexer
	indexers := toolscache.Indexers{
		toolscache.NamespaceIndex: toolscache.MetaNamespaceIndexFunc,
	}

	indexer, err := NewBBoltIndexer(store, &corev1.Secret{}, WithIndexers(indexers))
	if err != nil {
		t.Fatalf("failed to create indexer: %v", err)
	}

	sec1 := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sec-1",
			Namespace: "team-a",
		},
	}
	sec2 := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sec-2",
			Namespace: "team-a",
		},
	}
	sec3 := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sec-3",
			Namespace: "team-b",
		},
	}

	_ = indexer.Add(sec1)
	_ = indexer.Add(sec2)
	_ = indexer.Add(sec3)

	// ByIndex
	teamAObjs, err := indexer.ByIndex(toolscache.NamespaceIndex, "team-a")
	if err != nil {
		t.Fatalf("ByIndex failed: %v", err)
	}
	if len(teamAObjs) != 2 {
		t.Fatalf("expected 2 items for team-a, got %d", len(teamAObjs))
	}

	// IndexKeys
	teamBKeys, err := indexer.IndexKeys(toolscache.NamespaceIndex, "team-b")
	if err != nil || len(teamBKeys) != 1 || teamBKeys[0] != "team-b/sec-3" {
		t.Fatalf("IndexKeys mismatch: keys=%v, err=%v", teamBKeys, err)
	}

	// ListIndexFuncValues
	nsValues := indexer.ListIndexFuncValues(toolscache.NamespaceIndex)
	if len(nsValues) != 2 {
		t.Fatalf("expected 2 distinct namespaces, got %v", nsValues)
	}

	// Delete sec1 and verify index updated
	_ = indexer.Delete(sec1)
	teamAObjs, err = indexer.ByIndex(toolscache.NamespaceIndex, "team-a")
	if err != nil || len(teamAObjs) != 1 {
		t.Fatalf("expected 1 item after delete, got %d (err: %v)", len(teamAObjs), err)
	}
}

func TestBBoltIndexer_WithEncryption(t *testing.T) {
	cipher, err := NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("failed to create cipher: %v", err)
	}

	store, err := NewStore()
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	indexer, err := NewBBoltIndexer(store, &corev1.Secret{}, WithIndexerCipher(cipher))
	if err != nil {
		t.Fatalf("failed to create indexer: %v", err)
	}

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tls-secret",
			Namespace: "default",
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSPrivateKeyKey: []byte("SUPER_CONFIDENTIAL_KEY"),
		},
	}

	if err := indexer.Add(sec); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Retrieve via indexer
	item, exists, err := indexer.GetByKey("default/tls-secret")
	if err != nil || !exists {
		t.Fatalf("GetByKey failed: exists=%v, err=%v", exists, err)
	}
	retrievedSec := item.(*corev1.Secret)
	if string(retrievedSec.Data[corev1.TLSPrivateKeyKey]) != "SUPER_CONFIDENTIAL_KEY" {
		t.Fatalf("decrypted data mismatch")
	}

	// Verify raw bytes stored in bbolt DB do NOT contain the plaintext key!
	raw, rawExists, err := store.Get(fmt.Sprintf("%T", sec), "default/tls-secret")
	if err != nil || !rawExists {
		t.Fatalf("failed to get raw: exists=%v, err=%v", rawExists, err)
	}
	if string(raw) == "SUPER_CONFIDENTIAL_KEY" {
		t.Fatal("security violation: raw database contains unencrypted secret key!")
	}

	// Verify key-binding: copying ciphertext to a different key must fail decryption!
	if err := store.Put(fmt.Sprintf("%T", sec), "default/swapped-secret", raw); err != nil {
		t.Fatalf("store.Put failed: %v", err)
	}
	_, _, err = indexer.GetByKey("default/swapped-secret")
	if err == nil {
		t.Fatal("security violation: swapped ciphertext under different key decrypted successfully!")
	}
}

func TestBBoltIndexer_ReplaceAndResourceVersion(t *testing.T) {
	store, err := NewStore()
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	indexer, err := NewBBoltIndexer(store, &corev1.ConfigMap{}, WithIndexers(toolscache.Indexers{
		toolscache.NamespaceIndex: toolscache.MetaNamespaceIndexFunc,
	}))
	if err != nil {
		t.Fatalf("failed to create indexer: %v", err)
	}

	cms := []any{
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns1"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "c2", Namespace: "ns2"}},
	}

	if err := indexer.Replace(cms, "5000"); err != nil {
		t.Fatalf("Replace failed: %v", err)
	}

	if indexer.LastStoreSyncResourceVersion() != "5000" {
		t.Fatalf("expected RV 5000, got %s", indexer.LastStoreSyncResourceVersion())
	}

	if len(indexer.ListKeys()) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(indexer.ListKeys()))
	}

	// Bookmark updates RV
	indexer.Bookmark("5010")
	if indexer.LastStoreSyncResourceVersion() != "5010" {
		t.Fatalf("expected RV 5010, got %s", indexer.LastStoreSyncResourceVersion())
	}
}
