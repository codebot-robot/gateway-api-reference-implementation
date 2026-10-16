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

package sni

import (
	"crypto/tls"
	"fmt"
	"testing"
)

func TestLRUClientSessionCache_BasicAndEviction(t *testing.T) {
	cache := NewLRUClientSessionCache(3)
	if cap := cache.Capacity(); cap != 3 {
		t.Fatalf("expected capacity 3, got %d", cap)
	}

	state1 := &tls.ClientSessionState{}
	state2 := &tls.ClientSessionState{}
	state3 := &tls.ClientSessionState{}
	state4 := &tls.ClientSessionState{}

	cache.Put("k1", state1)
	cache.Put("k2", state2)
	cache.Put("k3", state3)
	if cache.Len() != 3 {
		t.Fatalf("expected len 3, got %d", cache.Len())
	}

	// Access k1 to make it most recently used
	if _, ok := cache.Get("k1"); !ok {
		t.Fatalf("expected k1 to be present")
	}

	// Put k4, should evict k2 (oldest)
	cache.Put("k4", state4)
	if cache.Len() != 3 {
		t.Fatalf("expected len 3 after eviction, got %d", cache.Len())
	}

	if _, ok := cache.Get("k2"); ok {
		t.Errorf("expected k2 to be evicted")
	}
	if _, ok := cache.Get("k1"); !ok {
		t.Errorf("expected k1 to remain")
	}
	if _, ok := cache.Get("k3"); !ok {
		t.Errorf("expected k3 to remain")
	}
	if _, ok := cache.Get("k4"); !ok {
		t.Errorf("expected k4 to remain")
	}

	// Deletion via Put(key, nil)
	cache.Put("k1", nil)
	if cache.Len() != 2 {
		t.Fatalf("expected len 2 after nil put, got %d", cache.Len())
	}
	if _, ok := cache.Get("k1"); ok {
		t.Errorf("expected k1 to be deleted")
	}
}

func TestLRUClientSessionCache_SetCapacity_GrowAndShrink(t *testing.T) {
	cache := NewLRUClientSessionCache(2)
	cache.Put("k1", &tls.ClientSessionState{})
	cache.Put("k2", &tls.ClientSessionState{})

	// Grow to 5
	cache.SetCapacity(5)
	if cache.Capacity() != 5 {
		t.Fatalf("expected capacity 5, got %d", cache.Capacity())
	}
	if cache.Len() != 2 {
		t.Fatalf("expected len 2 preserved, got %d", cache.Len())
	}

	cache.Put("k3", &tls.ClientSessionState{})
	cache.Put("k4", &tls.ClientSessionState{})
	cache.Put("k5", &tls.ClientSessionState{})
	if cache.Len() != 5 {
		t.Fatalf("expected len 5, got %d", cache.Len())
	}

	// Access k1, k2, k3 so k4 and k5 become oldest
	cache.Get("k1")
	cache.Get("k2")
	cache.Get("k3")

	// Shrink to 3: should evict k4 and k5
	cache.SetCapacity(3)
	if cache.Capacity() != 3 {
		t.Fatalf("expected capacity 3, got %d", cache.Capacity())
	}
	if cache.Len() != 3 {
		t.Fatalf("expected len 3 after shrink, got %d", cache.Len())
	}

	for _, k := range []string{"k1", "k2", "k3"} {
		if _, ok := cache.Get(k); !ok {
			t.Errorf("expected %s to be retained", k)
		}
	}
	for _, k := range []string{"k4", "k5"} {
		if _, ok := cache.Get(k); ok {
			t.Errorf("expected %s to be evicted", k)
		}
	}
}

func TestLRUClientSessionCache_LargeScale(t *testing.T) {
	cache := NewLRUClientSessionCache(256)
	cache.SetCapacity(500)

	for i := 0; i < 500; i++ {
		cache.Put(fmt.Sprintf("target-%d", i), &tls.ClientSessionState{})
	}
	if cache.Len() != 500 {
		t.Fatalf("expected len 500, got %d", cache.Len())
	}

	for i := 0; i < 500; i++ {
		if _, ok := cache.Get(fmt.Sprintf("target-%d", i)); !ok {
			t.Fatalf("expected target-%d to be in cache", i)
		}
	}
}
