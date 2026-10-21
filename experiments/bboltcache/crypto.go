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
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

// Cipher defines an interface for symmetric payload encryption and decryption.
type Cipher interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

// NoopCipher passes data through without encryption.
type NoopCipher struct{}

func (NoopCipher) Encrypt(plaintext []byte) ([]byte, error) {
	return plaintext, nil
}

func (NoopCipher) Decrypt(ciphertext []byte) ([]byte, error) {
	return ciphertext, nil
}

// AESGCMCipher implements Cipher using AES-256 in GCM mode with an ephemeral key.
type AESGCMCipher struct {
	aead cipher.AEAD
}

// NewEphemeralAESGCMCipher creates a new AES-256-GCM cipher with a cryptographically
// random in-memory key generated at startup. The key is never persisted to disk.
func NewEphemeralAESGCMCipher() (*AESGCMCipher, error) {
	key := make([]byte, 32) // AES-256
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("failed to generate random AES-256 key: %w", err)
	}
	return NewAESGCMCipher(key)
}

// NewAESGCMCipher creates an AES-GCM cipher with the given key (must be 16, 24, or 32 bytes).
func NewAESGCMCipher(key []byte) (*AESGCMCipher, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM AEAD: %w", err)
	}
	return &AESGCMCipher{aead: gcm}, nil
}

// Encrypt encrypts plaintext using AES-GCM with a fresh random nonce.
// The nonce is prepended to the ciphertext.
func (c *AESGCMCipher) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}
	// gcm.Seal appends ciphertext and authentication tag to dst (nonce).
	return c.aead.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt verifies and decrypts ciphertext using AES-GCM.
func (c *AESGCMCipher) Decrypt(ciphertext []byte) ([]byte, error) {
	nonceSize := c.aead.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("ciphertext too short: missing nonce")
	}
	nonce := ciphertext[:nonceSize]
	data := ciphertext[nonceSize:]
	plaintext, err := c.aead.Open(nil, nonce, data, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt ciphertext: %w", err)
	}
	return plaintext, nil
}
