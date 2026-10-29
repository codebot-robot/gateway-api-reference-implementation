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

package acme

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"testing"
)

func TestDecodeEABKey(t *testing.T) {
	raw := []byte("01234567890123456789012345678901") // 32 bytes

	// 1. Raw URL encoding
	rawURLEnc := base64.RawURLEncoding.EncodeToString(raw)
	decoded, err := DecodeEABKey(rawURLEnc)
	if err != nil {
		t.Fatalf("failed to decode raw URL encoded EAB key: %v", err)
	}
	if !bytes.Equal(decoded, raw) {
		t.Errorf("decoded raw URL mismatch: %v vs %v", decoded, raw)
	}

	// 2. Standard URL encoding
	urlEnc := base64.URLEncoding.EncodeToString(raw)
	decoded, err = DecodeEABKey(urlEnc)
	if err != nil {
		t.Fatalf("failed to decode standard URL encoded EAB key: %v", err)
	}
	if !bytes.Equal(decoded, raw) {
		t.Errorf("decoded URL mismatch: %v vs %v", decoded, raw)
	}

	// 3. Standard base64 encoding
	stdEnc := base64.StdEncoding.EncodeToString(raw)
	decoded, err = DecodeEABKey(stdEnc)
	if err != nil {
		t.Fatalf("failed to decode std encoded EAB key: %v", err)
	}
	if !bytes.Equal(decoded, raw) {
		t.Errorf("decoded std mismatch: %v vs %v", decoded, raw)
	}

	// 4. Invalid base64
	_, err = DecodeEABKey("??? invalid !!!")
	if err == nil {
		t.Error("expected error for invalid base64 string")
	}
}

func TestNewClient_KeyGenerationAndCustomKey(t *testing.T) {
	ctx := t.Context()

	// 1. Key generated when nil
	cli1, err := NewClient(ctx, Config{
		DirectoryURL: "https://example.com/dir",
	})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if cli1.AccountKey == nil {
		t.Fatal("expected generated AccountKey")
	}
	if cli1.AccountKey.Curve != elliptic.P256() {
		t.Errorf("expected P-256 curve, got %v", cli1.AccountKey.Curve.Params().Name)
	}

	// 2. Custom key retained
	customKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate custom key: %v", err)
	}
	cli2, err := NewClient(ctx, Config{
		DirectoryURL: "https://example.com/dir",
		AccountKey:   customKey,
	})
	if err != nil {
		t.Fatalf("NewClient with custom key failed: %v", err)
	}
	if cli2.AccountKey != customKey {
		t.Error("custom AccountKey was not retained")
	}
}
