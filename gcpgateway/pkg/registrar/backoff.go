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

package registrar

import (
	"math/rand"
	"sync"
	"time"
)

const (
	// DefaultMinBackoff is the initial backoff duration after a failure.
	DefaultMinBackoff = 1 * time.Second

	// DefaultMaxBackoff is the maximum capped backoff duration.
	DefaultMaxBackoff = 60 * time.Second
)

// BackoffTracker manages exponential backoff per key (such as Gateway namespace/name).
type BackoffTracker struct {
	// mu guards states and rand.
	mu         sync.Mutex
	minBackoff time.Duration
	maxBackoff time.Duration
	rand       *rand.Rand
	states     map[string]*backoffState
}

type backoffState struct {
	baseDuration time.Duration
	retryAfter   time.Time
	lastErr      error
}

// NewBackoffTracker creates a new BackoffTracker with the given min and max backoff durations.
func NewBackoffTracker(minBackoff, maxBackoff time.Duration) *BackoffTracker {
	if minBackoff <= 0 {
		minBackoff = DefaultMinBackoff
	}
	if maxBackoff <= 0 {
		maxBackoff = DefaultMaxBackoff
	}
	if maxBackoff < minBackoff {
		maxBackoff = minBackoff
	}
	return &BackoffTracker{
		minBackoff: minBackoff,
		maxBackoff: maxBackoff,
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
		states:     make(map[string]*backoffState),
	}
}

// Remaining returns the duration remaining in backoff if key is in backoff at time now.
// If not in backoff or backoff has elapsed, it returns 0.
func (b *BackoffTracker) Remaining(key string, now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	st, exists := b.states[key]
	if !exists {
		return 0
	}
	if now.Before(st.retryAfter) {
		return st.retryAfter.Sub(now)
	}
	return 0
}

// RecordFailure updates the backoff state for key after a failure, doubling the backoff
// duration up to maxBackoff with ±10% jitter, and returns the new backoff duration.
func (b *BackoffTracker) RecordFailure(key string, err error, now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.rand == nil {
		b.rand = rand.New(rand.NewSource(now.UnixNano()))
	}

	st, exists := b.states[key]
	var base time.Duration
	if !exists {
		base = b.minBackoff
		st = &backoffState{baseDuration: base}
		b.states[key] = st
	} else {
		base = st.baseDuration * 2
		if base > b.maxBackoff {
			base = b.maxBackoff
		}
		st.baseDuration = base
	}

	// Apply ±10% jitter: factor between 0.9 and 1.1
	jitterFactor := 0.9 + 0.2*b.rand.Float64()
	duration := time.Duration(float64(base) * jitterFactor)
	if duration > b.maxBackoff {
		duration = b.maxBackoff
	}

	st.retryAfter = now.Add(duration)
	st.lastErr = err
	return duration
}

// RecordSuccess clears the backoff state for key after a successful operation.
func (b *BackoffTracker) RecordSuccess(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.states, key)
}

// Clear removes backoff state for key.
func (b *BackoffTracker) Clear(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.states, key)
}

// LastError returns the most recent error recorded for key, if any.
func (b *BackoffTracker) LastError(key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if st, exists := b.states[key]; exists {
		return st.lastErr
	}
	return nil
}
