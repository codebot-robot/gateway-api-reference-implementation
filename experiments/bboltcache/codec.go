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
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ObjectCodec serializes and deserializes Kubernetes objects for disk storage.
type ObjectCodec interface {
	Encode(obj runtime.Object) ([]byte, error)
	Decode(data []byte, into runtime.Object) error
}

// StripManagedFields removes managedFields from an object if it implements metav1.Object.
// Managed fields can account for 50%+ of metadata size and are never needed by GARI recompute.
func StripManagedFields(obj any) {
	if metaObj, ok := obj.(metav1.Object); ok {
		metaObj.SetManagedFields(nil)
	}
}

// JSONCodec serializes objects to JSON, stripping managed fields beforehand.
type JSONCodec struct{}

// NewJSONCodec creates a new JSONCodec.
func NewJSONCodec() *JSONCodec {
	return &JSONCodec{}
}

// Encode strips managed fields and serializes the object to JSON bytes.
func (c *JSONCodec) Encode(obj runtime.Object) ([]byte, error) {
	if obj == nil {
		return nil, fmt.Errorf("cannot encode nil object")
	}
	StripManagedFields(obj)
	return json.Marshal(obj)
}

// Decode deserializes JSON bytes into the target runtime.Object.
func (c *JSONCodec) Decode(data []byte, into runtime.Object) error {
	if into == nil {
		return fmt.Errorf("cannot decode into nil target")
	}
	if err := json.Unmarshal(data, into); err != nil {
		return fmt.Errorf("failed to decode object: %w", err)
	}
	return nil
}
