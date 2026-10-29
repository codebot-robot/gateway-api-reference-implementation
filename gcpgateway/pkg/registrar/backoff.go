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
	mu         sync.Mutex
	minBackoff time.Duration
	maxBackoff time.Duration
	states     map[string]*backoffState
}

type backoffState struct {
	currentDuration time.Duration
	retryAfter      time.Time
	lastErr         error
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
		states:     make(map[string]*backoffState),
	}
}

// Remaining returns the duration remaining in backoff and the last error if key is in backoff at time now.
// If not in backoff or backoff has elapsed, it returns 0 and nil.
func (b *BackoffTracker) Remaining(key string, now time.Time) (time.Duration, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	st, exists := b.states[key]
	if !exists {
		return 0, nil
	}
	if now.Before(st.retryAfter) {
		return st.retryAfter.Sub(now), st.lastErr
	}
	return 0, nil
}

// RecordFailure updates the backoff state for key after a failure, doubling the backoff
// duration up to maxBackoff, and returns the new backoff duration.
func (b *BackoffTracker) RecordFailure(key string, err error, now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	st, exists := b.states[key]
	var duration time.Duration
	if !exists {
		duration = b.minBackoff
		b.states[key] = &backoffState{
			currentDuration: duration,
			retryAfter:      now.Add(duration),
			lastErr:         err,
		}
		return duration
	}

	duration = st.currentDuration * 2
	if duration > b.maxBackoff {
		duration = b.maxBackoff
	}
	st.currentDuration = duration
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

// LastError returns the most recent error recorded for key.
func (b *BackoffTracker) LastError(key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if st, exists := b.states[key]; exists {
		return st.lastErr
	}
	return nil
}
