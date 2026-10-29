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
	"errors"
	"testing"
	"time"
)

func TestBackoffTracker(t *testing.T) {
	minB := 100 * time.Millisecond
	maxB := 1000 * time.Millisecond
	tracker := NewBackoffTracker(minB, maxB)

	key := "default/my-gw"
	errFoo := errors.New("403 Forbidden")
	t0 := time.Now()

	// Initially not in backoff
	rem := tracker.Remaining(key, t0)
	if rem != 0 {
		t.Fatalf("expected 0 remaining initially, got %v", rem)
	}

	// 1st failure: minBackoff (100ms) with ±10% jitter => [90ms, 110ms]
	d1 := tracker.RecordFailure(key, errFoo, t0)
	if d1 < 90*time.Millisecond || d1 > 110*time.Millisecond {
		t.Errorf("expected 1st backoff in [90ms, 110ms], got %v", d1)
	}
	if !errors.Is(tracker.LastError(key), errFoo) {
		t.Errorf("expected last error %v, got %v", errFoo, tracker.LastError(key))
	}

	// 50ms later: in backoff, remaining should be positive and <= d1
	rem = tracker.Remaining(key, t0.Add(50*time.Millisecond))
	if rem <= 0 || rem > d1 {
		t.Errorf("expected positive remaining <= %v, got %v", d1, rem)
	}

	// After backoff duration: backoff expired
	rem = tracker.Remaining(key, t0.Add(d1+time.Millisecond))
	if rem != 0 {
		t.Errorf("expected 0 remaining after backoff expired, got %v", rem)
	}

	// 2nd failure: doubled to 200ms with ±10% jitter => [180ms, 220ms]
	t1 := t0.Add(d1)
	d2 := tracker.RecordFailure(key, errFoo, t1)
	if d2 < 180*time.Millisecond || d2 > 220*time.Millisecond {
		t.Errorf("expected 2nd backoff in [180ms, 220ms], got %v", d2)
	}

	// 3rd failure: doubled to 400ms with ±10% jitter => [360ms, 440ms]
	t2 := t1.Add(d2)
	d3 := tracker.RecordFailure(key, errFoo, t2)
	if d3 < 360*time.Millisecond || d3 > 440*time.Millisecond {
		t.Errorf("expected 3rd backoff in [360ms, 440ms], got %v", d3)
	}

	// 4th failure: doubled to 800ms with ±10% jitter => [720ms, 880ms]
	t3 := t2.Add(d3)
	d4 := tracker.RecordFailure(key, errFoo, t3)
	if d4 < 720*time.Millisecond || d4 > 880*time.Millisecond {
		t.Errorf("expected 4th backoff in [720ms, 880ms], got %v", d4)
	}

	// 5th failure: base 1600ms capped at maxB (1000ms), duration <= maxB
	t4 := t3.Add(d4)
	d5 := tracker.RecordFailure(key, errFoo, t4)
	if d5 < 900*time.Millisecond || d5 > maxB {
		t.Errorf("expected 5th backoff capped at <= %v, got %v", maxB, d5)
	}

	// RecordSuccess clears backoff
	tracker.RecordSuccess(key)
	rem = tracker.Remaining(key, t4.Add(10*time.Millisecond))
	if rem != 0 {
		t.Errorf("expected 0 remaining after RecordSuccess, got %v", rem)
	}

	// Next failure after success starts from minBackoff again
	dAfterReset := tracker.RecordFailure(key, errFoo, t4.Add(10*time.Millisecond))
	if dAfterReset < 90*time.Millisecond || dAfterReset > 110*time.Millisecond {
		t.Errorf("expected backoff reset to [90ms, 110ms] after success, got %v", dAfterReset)
	}

	// Clear removes backoff state
	tracker.Clear(key)
	rem = tracker.Remaining(key, t4.Add(20*time.Millisecond))
	if rem != 0 {
		t.Errorf("expected 0 remaining after Clear, got %v", rem)
	}
}
