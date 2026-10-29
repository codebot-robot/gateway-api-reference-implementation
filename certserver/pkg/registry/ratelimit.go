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

package registry

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// IdentityRateLimiter provides rate limiting partitioned per CallerIdentity.
type IdentityRateLimiter struct {
	r rate.Limit
	b int

	// mu guards limiters and lastSeen.
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	lastSeen map[string]time.Time
}

// NewIdentityRateLimiter creates a rate limiter allowing r requests per second with burst b per identity.
// If r <= 0, rate limiting is disabled (all requests allowed).
func NewIdentityRateLimiter(r rate.Limit, b int) *IdentityRateLimiter {
	return &IdentityRateLimiter{
		limiters: make(map[string]*rate.Limiter),
		lastSeen: make(map[string]time.Time),
		r:        r,
		b:        b,
	}
}

// Allow reports whether a request from identity is allowed by its rate limiter.
func (l *IdentityRateLimiter) Allow(id CallerIdentity) bool {
	if l.r <= 0 {
		return true
	}

	key := id.String()
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	limiter, exists := l.limiters[key]
	if !exists {
		// Clean up old entries if map grows large (> 5000 entries)
		if len(l.limiters) > 5000 {
			for k, seen := range l.lastSeen {
				if now.Sub(seen) > 10*time.Minute {
					delete(l.limiters, k)
					delete(l.lastSeen, k)
				}
			}
		}

		limiter = rate.NewLimiter(l.r, l.b)
		l.limiters[key] = limiter
	}
	l.lastSeen[key] = now

	return limiter.Allow()
}
