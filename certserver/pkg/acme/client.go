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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"golang.org/x/crypto/acme"
)

// Config configures the ACME client.
type Config struct {
	DirectoryURL string
	EABKID       string
	EABHMACKey   string
	AccountKey   *ecdsa.PrivateKey
	HTTPClient   *http.Client
	UserAgent    string
}

// Client wraps acme.Client with lazy account lifecycle and EAB support.
type Client struct {
	*acme.Client
	AccountKey *ecdsa.PrivateKey
	cfg        Config

	// mu guards lazy registration of Account.
	mu      sync.Mutex
	Account *acme.Account
}

// NewClient initializes an ACME client with an ECDSA P-256 account key and configuration.
// Network registration of the account is deferred until EnsureAccount or first order.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	accountKey := cfg.AccountKey
	if accountKey == nil {
		var err error
		accountKey, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("failed to generate ECDSA P-256 account key: %w", err)
		}
	}

	acmeClient := &acme.Client{
		Key:          accountKey,
		DirectoryURL: cfg.DirectoryURL,
		HTTPClient:   cfg.HTTPClient,
		UserAgent:    cfg.UserAgent,
	}

	c := &Client{
		Client:     acmeClient,
		AccountKey: accountKey,
		cfg:        cfg,
	}

	return c, nil
}

// EnsureAccount registers or retrieves the ACME account with the CA, including EAB if configured.
func (c *Client) EnsureAccount(ctx context.Context) (*acme.Account, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Account != nil {
		return c.Account, nil
	}

	reqAcct := &acme.Account{}
	if c.cfg.EABKID != "" && c.cfg.EABHMACKey != "" {
		hmacKeyBytes, err := DecodeEABKey(c.cfg.EABHMACKey)
		if err != nil {
			return nil, fmt.Errorf("invalid EAB HMAC key: %w", err)
		}
		reqAcct.ExternalAccountBinding = &acme.ExternalAccountBinding{
			KID: c.cfg.EABKID,
			Key: hmacKeyBytes,
		}
	}

	acct, err := c.Client.Register(ctx, reqAcct, acme.AcceptTOS)
	if err != nil {
		if errors.Is(err, acme.ErrAccountAlreadyExists) {
			acct, err = c.Client.GetReg(ctx, "")
			if err != nil {
				return nil, fmt.Errorf("failed to get existing ACME registration: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to register ACME account: %w", err)
		}
	}

	if acct != nil && acct.URI != "" {
		c.Client.KID = acme.KeyID(acct.URI)
	}
	c.Account = acct

	return acct, nil
}

// DecodeEABKey parses an External Account Binding HMAC key from base64url or base64.
func DecodeEABKey(keyStr string) ([]byte, error) {
	encodings := []*base64.Encoding{
		base64.RawURLEncoding,
		base64.URLEncoding,
		base64.RawStdEncoding,
		base64.StdEncoding,
	}

	var lastErr error
	for _, enc := range encodings {
		b, err := enc.DecodeString(keyStr)
		if err == nil {
			return b, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("cannot decode EAB HMAC key with standard base64 or base64url encoding: %w", lastErr)
}
