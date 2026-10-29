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
	rem, err := tracker.Remaining(key, t0)
	if rem != 0 || err != nil {
		t.Fatalf("expected 0 remaining and nil error initially, got %v, %v", rem, err)
	}

	// 1st failure: minBackoff (100ms)
	d1 := tracker.RecordFailure(key, errFoo, t0)
	if d1 != minB {
		t.Errorf("expected 1st backoff %v, got %v", minB, d1)
	}

	// 50ms later: in backoff, ~50ms remaining
	rem, err = tracker.Remaining(key, t0.Add(50*time.Millisecond))
	if rem <= 0 || rem > 50*time.Millisecond {
		t.Errorf("expected ~50ms remaining, got %v", rem)
	}
	if !errors.Is(err, errFoo) {
		t.Errorf("expected error %v, got %v", errFoo, err)
	}

	// 100ms later: backoff expired
	rem, err = tracker.Remaining(key, t0.Add(100*time.Millisecond))
	if rem != 0 || err != nil {
		t.Errorf("expected 0 remaining after 100ms, got %v, %v", rem, err)
	}

	// 2nd failure: doubled to 200ms
	t1 := t0.Add(100 * time.Millisecond)
	d2 := tracker.RecordFailure(key, errFoo, t1)
	if d2 != 200*time.Millisecond {
		t.Errorf("expected 2nd backoff 200ms, got %v", d2)
	}

	// 3rd failure: doubled to 400ms
	t2 := t1.Add(200 * time.Millisecond)
	d3 := tracker.RecordFailure(key, errFoo, t2)
	if d3 != 400*time.Millisecond {
		t.Errorf("expected 3rd backoff 400ms, got %v", d3)
	}

	// 4th failure: doubled to 800ms
	t3 := t2.Add(400 * time.Millisecond)
	d4 := tracker.RecordFailure(key, errFoo, t3)
	if d4 != 800*time.Millisecond {
		t.Errorf("expected 4th backoff 800ms, got %v", d4)
	}

	// 5th failure: capped at maxB (1000ms, not 1600ms)
	t4 := t3.Add(800 * time.Millisecond)
	d5 := tracker.RecordFailure(key, errFoo, t4)
	if d5 != maxB {
		t.Errorf("expected 5th backoff capped at %v, got %v", maxB, d5)
	}

	// RecordSuccess clears backoff
	tracker.RecordSuccess(key)
	rem, err = tracker.Remaining(key, t4.Add(10*time.Millisecond))
	if rem != 0 || err != nil {
		t.Errorf("expected 0 remaining after RecordSuccess, got %v, %v", rem, err)
	}

	// Next failure after success starts from minBackoff again
	dAfterReset := tracker.RecordFailure(key, errFoo, t4.Add(10*time.Millisecond))
	if dAfterReset != minB {
		t.Errorf("expected backoff reset to %v after success, got %v", minB, dAfterReset)
	}

	// Clear removes backoff state
	tracker.Clear(key)
	rem, err = tracker.Remaining(key, t4.Add(20*time.Millisecond))
	if rem != 0 || err != nil {
		t.Errorf("expected 0 remaining after Clear, got %v, %v", rem, err)
	}
}
