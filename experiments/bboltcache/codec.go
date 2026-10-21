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

const (
	formatProtobuf byte = 0x01
	formatJSON     byte = 0x02
)

// protoMarshaler is implemented by Kubernetes core types (e.g., ConfigMap, Secret).
type protoMarshaler interface {
	Marshal() ([]byte, error)
}

// protoUnmarshaler is implemented by Kubernetes core types (e.g., ConfigMap, Secret).
type protoUnmarshaler interface {
	Unmarshal([]byte) error
}

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

// TransformFunc is a client-go informer transform function that strips managedFields on ingestion
// so objects in the DeltaFIFO and store are already stripped without mutating handler arguments.
func TransformFunc(obj any) (any, error) {
	StripManagedFields(obj)
	return obj, nil
}

// HybridCodec serializes objects using Protobuf for core Kubernetes types (such as ConfigMap and Secret)
// for maximum speed and compaction, falling back to JSON for types without protobuf generation.
//
// It operates on a deep-copied object before stripping managedFields to ensure the informer's original
// object passed to event handlers is never mutated.
type HybridCodec struct{}

// NewHybridCodec creates a new HybridCodec.
func NewHybridCodec() *HybridCodec {
	return &HybridCodec{}
}

// Encode strips managed fields from a copy of the object and serializes it with Protobuf or JSON.
func (c *HybridCodec) Encode(obj runtime.Object) ([]byte, error) {
	if obj == nil {
		return nil, fmt.Errorf("cannot encode nil object")
	}

	// Never mutate the original informer object; strip on a copy.
	cloned := obj.DeepCopyObject()
	StripManagedFields(cloned)

	// Prefer protobuf for core types
	if pm, ok := cloned.(protoMarshaler); ok {
		raw, err := pm.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to encode object to protobuf: %w", err)
		}
		out := make([]byte, 1+len(raw))
		out[0] = formatProtobuf
		copy(out[1:], raw)
		return out, nil
	}

	// Fallback to JSON
	raw, err := json.Marshal(cloned)
	if err != nil {
		return nil, fmt.Errorf("failed to encode object to json: %w", err)
	}
	out := make([]byte, 1+len(raw))
	out[0] = formatJSON
	copy(out[1:], raw)
	return out, nil
}

// Decode deserializes bytes (Protobuf or JSON) into the target runtime.Object.
func (c *HybridCodec) Decode(data []byte, into runtime.Object) error {
	if into == nil {
		return fmt.Errorf("cannot decode into nil target")
	}
	if len(data) == 0 {
		return fmt.Errorf("cannot decode empty data")
	}

	switch data[0] {
	case formatProtobuf:
		if pu, ok := into.(protoUnmarshaler); ok {
			if err := pu.Unmarshal(data[1:]); err != nil {
				return fmt.Errorf("failed to decode protobuf payload: %w", err)
			}
			return nil
		}
		return fmt.Errorf("target %T does not implement protobuf unmarshaling", into)

	case formatJSON:
		if err := json.Unmarshal(data[1:], into); err != nil {
			return fmt.Errorf("failed to decode json payload: %w", err)
		}
		return nil

	default:
		// Backwards compatibility for raw JSON without format prefix
		if err := json.Unmarshal(data, into); err != nil {
			return fmt.Errorf("failed to decode payload: %w", err)
		}
		return nil
	}
}

// JSONCodec serializes objects exclusively to JSON (for comparison or testing).
type JSONCodec struct{}

// NewJSONCodec creates a new JSONCodec.
func NewJSONCodec() *JSONCodec {
	return &JSONCodec{}
}

// Encode serializes a deep copy of the object stripped of managed fields to JSON.
func (c *JSONCodec) Encode(obj runtime.Object) ([]byte, error) {
	if obj == nil {
		return nil, fmt.Errorf("cannot encode nil object")
	}
	cloned := obj.DeepCopyObject()
	StripManagedFields(cloned)
	return json.Marshal(cloned)
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
