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
	// DefaultHysteresisWindow is the duration an unready, non-terminating pod remains attached before detachment.
	DefaultHysteresisWindow = 15 * time.Second
)

// HysteresisTracker manages flapping hysteresis for pods that lose container readiness.
type HysteresisTracker struct {
	mu           sync.Mutex
	window       time.Duration
	unreadySince map[string]time.Time
}

// NewHysteresisTracker creates a new HysteresisTracker with the given window (or DefaultHysteresisWindow if <= 0).
func NewHysteresisTracker(window time.Duration) *HysteresisTracker {
	if window <= 0 {
		window = DefaultHysteresisWindow
	}
	return &HysteresisTracker{
		window:       window,
		unreadySince: make(map[string]time.Time),
	}
}

// ShouldDetach evaluates the pod's readiness state and returns true if the pod must be detached from NEGs.
// - Terminating pods detach immediately (returns true).
// - Pods with ContainersReady=true stay attached (returns false).
// - Non-terminating pods with ContainersReady=false remain attached until the hysteresis window elapses.
func (h *HysteresisTracker) ShouldDetach(podKey string, containersReady bool, isTerminating bool, now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 1. Terminating pods detach immediately
	if isTerminating {
		delete(h.unreadySince, podKey)
		return true
	}

	// 2. Ready pods clear any unready timer and remain attached
	if containersReady {
		delete(h.unreadySince, podKey)
		return false
	}

	// 3. Unready, non-terminating pods observe the hysteresis window
	since, exists := h.unreadySince[podKey]
	if !exists {
		h.unreadySince[podKey] = now
		return false
	}

	if now.Sub(since) >= h.window {
		return true
	}

	return false
}

// Clear removes a pod from the hysteresis tracker.
func (h *HysteresisTracker) Clear(podKey string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.unreadySince, podKey)
}
