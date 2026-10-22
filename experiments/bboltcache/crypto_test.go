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
	"bytes"
	"testing"
)

func TestAESGCMCipher_EncryptDecrypt(t *testing.T) {
	cipher, err := NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("failed to create ephemeral cipher: %v", err)
	}

	ad := AdditionalData("Secret", "default/tls-key")
	plaintext := []byte("secret-tls-private-key-data-1234567890")
	ciphertext, err := cipher.Encrypt(plaintext, ad)
	if err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	if bytes.Equal(plaintext, ciphertext) {
		t.Fatal("ciphertext matches plaintext; data was not encrypted")
	}

	decrypted, err := cipher.Decrypt(ciphertext, ad)
	if err != nil {
		t.Fatalf("failed to decrypt: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Fatalf("decrypted data %q != original %q", decrypted, plaintext)
	}
}

func TestAESGCMCipher_KeyBinding_SwapFails(t *testing.T) {
	cipher, err := NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("failed to create cipher: %v", err)
	}

	adKeyA := AdditionalData("Secret", "tenant-a/secret-token")
	adKeyB := AdditionalData("Secret", "tenant-b/secret-token")

	plaintext := []byte("confidential-token-for-tenant-a")
	ciphertext, err := cipher.Encrypt(plaintext, adKeyA)
	if err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	// Attempting to decrypt the ciphertext under tenant-b's key MUST fail
	_, err = cipher.Decrypt(ciphertext, adKeyB)
	if err == nil {
		t.Fatal("security violation: ciphertext encrypted for key A successfully decrypted under key B!")
	}
}

func TestAESGCMCipher_TamperDetection(t *testing.T) {
	cipher, err := NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("failed to create cipher: %v", err)
	}

	ad := AdditionalData("ConfigMap", "default/cfg")
	plaintext := []byte("important-cluster-data")
	ciphertext, err := cipher.Encrypt(plaintext, ad)
	if err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	// Tamper with the ciphertext byte
	tampered := bytes.Clone(ciphertext)
	tampered[len(tampered)-1] ^= 0x01

	_, err = cipher.Decrypt(tampered, ad)
	if err == nil {
		t.Fatal("expected decryption failure for tampered ciphertext, but got nil")
	}
}

func TestAESGCMCipher_DifferentKeysCannotDecrypt(t *testing.T) {
	cipher1, err := NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("failed to create cipher 1: %v", err)
	}
	cipher2, err := NewEphemeralAESGCMCipher()
	if err != nil {
		t.Fatalf("failed to create cipher 2: %v", err)
	}

	ad := AdditionalData("Secret", "ns/sec")
	plaintext := []byte("data-for-cipher-1")
	ct, err := cipher1.Encrypt(plaintext, ad)
	if err != nil {
		t.Fatalf("failed to encrypt: %v", err)
	}

	_, err = cipher2.Decrypt(ct, ad)
	if err == nil {
		t.Fatal("expected cipher2 to fail decrypting cipher1's ciphertext")
	}
}

func TestNoopCipher(t *testing.T) {
	c := NoopCipher{}
	ad := AdditionalData("b", "k")
	data := []byte("plain")
	enc, err := c.Encrypt(data, ad)
	if err != nil || !bytes.Equal(data, enc) {
		t.Fatalf("NoopCipher.Encrypt mismatch: %v, %v", enc, err)
	}
	dec, err := c.Decrypt(enc, ad)
	if err != nil || !bytes.Equal(data, dec) {
		t.Fatalf("NoopCipher.Decrypt mismatch: %v, %v", dec, err)
	}
}
