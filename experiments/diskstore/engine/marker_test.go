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

package engine_test

import (
	"errors"
	"testing"

	"github.com/gke-labs/gateway-api-reference-implementation/experiments/diskstore/engine"
)

func TestEngine_FailureMarkerRecovery(t *testing.T) {
	memFS := engine.NewMemFS()
	dir := "test_failure_marker_dir"

	// 1. Initial run: write objects with per-bucket progress tokens
	eng1, err := engine.Open(engine.WithFS(memFS), engine.WithDir(dir))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if eng1.Wiped() {
		t.Fatalf("initial open should not report Wiped()")
	}

	origDBID := eng1.DBID()

	if err := eng1.Put("bucketA", "k1", []byte("val1"), "token-A-1"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := eng1.Put("bucketB", "k2", []byte("val2"), "token-B-1"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := eng1.RecordBookmark("bucketA", "token-A-2"); err != nil {
		t.Fatalf("RecordBookmark failed: %v", err)
	}
	if err := eng1.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	if tok := eng1.Token("bucketA"); tok != "token-A-2" {
		t.Fatalf("expected token-A-2, got %q", tok)
	}
	if tok := eng1.Token("bucketB"); tok != "token-B-1" {
		t.Fatalf("expected token-B-1, got %q", tok)
	}

	// 2. Simulate a failure: transition into failed state, which writes the failure marker
	eng1.SimulateFailure(errors.New("simulated fatal corruption"))

	select {
	case <-eng1.Failed():
		// Success: channel closed
	default:
		t.Fatalf("expected Failed() channel to be closed after failure")
	}

	if err := eng1.Err(); err == nil || !errors.Is(err, engine.ErrFailed) {
		t.Fatalf("expected ErrFailed, got %v", err)
	}

	// Any operation on failed engine must fail
	if err := eng1.Put("bucketA", "k3", []byte("val3"), "token-3"); !errors.Is(err, engine.ErrFailed) {
		t.Fatalf("expected ErrFailed on Put after failure, got %v", err)
	}

	_ = eng1.Close()

	// 3. Reopen: engine must detect the failure marker, wipe the store, report the wipe,
	// generate a new DB ID, and return no tokens.
	eng2, err := engine.Open(engine.WithFS(memFS), engine.WithDir(dir))
	if err != nil {
		t.Fatalf("reopening store after failure failed: %v", err)
	}
	defer eng2.Close()

	if !eng2.Wiped() {
		t.Fatalf("expected eng2.Wiped() to be true after failure marker detection")
	}

	if eng2.DBID() == origDBID {
		t.Fatalf("expected new database ID after wipe, got same ID %x", origDBID)
	}

	tokens := eng2.Tokens()
	if len(tokens) != 0 {
		t.Fatalf("expected no tokens after wipe, got %v", tokens)
	}

	if tok := eng2.Token("bucketA"); tok != "" {
		t.Fatalf("expected empty token for bucketA after wipe, got %q", tok)
	}
	if tok := eng2.Token("bucketB"); tok != "" {
		t.Fatalf("expected empty token for bucketB after wipe, got %q", tok)
	}

	keys := eng2.ListKeys()
	if len(keys) != 0 {
		t.Fatalf("expected 0 keys after wipe, got %v", keys)
	}

	// 4. Verify fresh engine works normally and saves tokens
	if err := eng2.Put("bucketC", "k4", []byte("val4"), "token-C-1"); err != nil {
		t.Fatalf("Put on wiped store failed: %v", err)
	}
	if err := eng2.Sync(); err != nil {
		t.Fatalf("Sync on wiped store failed: %v", err)
	}

	// 5. Subsequent clean reopen must NOT report wiped and must preserve tokens
	_ = eng2.Close()

	eng3, err := engine.Open(engine.WithFS(memFS), engine.WithDir(dir))
	if err != nil {
		t.Fatalf("second reopen failed: %v", err)
	}
	defer eng3.Close()

	if eng3.Wiped() {
		t.Fatalf("clean reopen should NOT report Wiped()")
	}
	if tok := eng3.Token("bucketC"); tok != "token-C-1" {
		t.Fatalf("expected token-C-1 after clean reopen, got %q", tok)
	}
}
