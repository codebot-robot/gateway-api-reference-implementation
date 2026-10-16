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
	"container/list"
	"crypto/tls"
	"sync"
)

type lruEntry struct {
	key   string
	state *tls.ClientSessionState
}

// LRUClientSessionCache is a thread-safe, resizable LRU cache implementing tls.ClientSessionCache.
type LRUClientSessionCache struct {
	mu       sync.Mutex
	capacity int
	m        map[string]*list.Element
	q        *list.List
}

// NewLRUClientSessionCache returns a new LRUClientSessionCache with the given capacity.
func NewLRUClientSessionCache(capacity int) *LRUClientSessionCache {
	const minCapacity = 1
	if capacity < minCapacity {
		capacity = minCapacity
	}
	return &LRUClientSessionCache{
		capacity: capacity,
		m:        make(map[string]*list.Element),
		q:        list.New(),
	}
}

// SetCapacity dynamically resizes the cache capacity, evicting the oldest entries
// if the current length exceeds the new capacity while retaining existing entries.
func (c *LRUClientSessionCache) SetCapacity(capacity int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	const minCapacity = 1
	if capacity < minCapacity {
		capacity = minCapacity
	}
	c.capacity = capacity
	for c.q.Len() > c.capacity {
		c.evictOldestLocked()
	}
}

// Capacity returns the current capacity of the cache.
func (c *LRUClientSessionCache) Capacity() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capacity
}

// Len returns the current number of cached entries.
func (c *LRUClientSessionCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.q.Len()
}

// Put adds or updates a session in the cache. A nil cs removes the entry.
func (c *LRUClientSessionCache) Put(sessionKey string, cs *tls.ClientSessionState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.m[sessionKey]; ok {
		if cs == nil {
			c.q.Remove(elem)
			delete(c.m, sessionKey)
			return
		}
		elem.Value.(*lruEntry).state = cs
		c.q.MoveToFront(elem)
		return
	}

	if cs == nil {
		return
	}

	elem := c.q.PushFront(&lruEntry{key: sessionKey, state: cs})
	c.m[sessionKey] = elem

	for c.q.Len() > c.capacity {
		c.evictOldestLocked()
	}
}

// Get retrieves a session state from the cache.
func (c *LRUClientSessionCache) Get(sessionKey string) (*tls.ClientSessionState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.m[sessionKey]; ok {
		c.q.MoveToFront(elem)
		return elem.Value.(*lruEntry).state, true
	}
	return nil, false
}

func (c *LRUClientSessionCache) evictOldestLocked() {
	elem := c.q.Back()
	if elem == nil {
		return
	}
	c.q.Remove(elem)
	entry := elem.Value.(*lruEntry)
	delete(c.m, entry.key)
}
