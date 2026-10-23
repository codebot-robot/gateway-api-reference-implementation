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

package kubecache

import (
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ObjectCodec serializes and deserializes Kubernetes runtime.Object instances.
type ObjectCodec interface {
	Encode(obj runtime.Object) ([]byte, error)
	Decode(data []byte, into runtime.Object) error
}

// JSONCodec implements ObjectCodec using standard JSON.
type JSONCodec struct{}

// NewJSONCodec creates a JSONCodec.
func NewJSONCodec() *JSONCodec {
	return &JSONCodec{}
}

func (j *JSONCodec) Encode(obj runtime.Object) ([]byte, error) {
	clone := obj.DeepCopyObject()
	if metaObj, ok := clone.(metav1.Object); ok {
		metaObj.SetManagedFields(nil)
	}
	return json.Marshal(clone)
}

func (j *JSONCodec) Decode(data []byte, into runtime.Object) error {
	return json.Unmarshal(data, into)
}

// HybridCodec uses optimized protobuf serialization for core types (ConfigMap, Secret)
// and falls back to JSON for other types.
type HybridCodec struct {
	jsonCodec *JSONCodec
}

// NewHybridCodec creates a HybridCodec.
func NewHybridCodec() *HybridCodec {
	return &HybridCodec{jsonCodec: NewJSONCodec()}
}

func (h *HybridCodec) Encode(obj runtime.Object) ([]byte, error) {
	switch o := obj.(type) {
	case *corev1.ConfigMap:
		clone := o.DeepCopy()
		clone.ManagedFields = nil
		data, err := clone.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to protobuf marshal ConfigMap: %w", err)
		}
		// Prefix 0x01 indicates protobuf ConfigMap
		res := make([]byte, 1+len(data))
		res[0] = 1
		copy(res[1:], data)
		return res, nil

	case *corev1.Secret:
		clone := o.DeepCopy()
		clone.ManagedFields = nil
		data, err := clone.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to protobuf marshal Secret: %w", err)
		}
		// Prefix 0x02 indicates protobuf Secret
		res := make([]byte, 1+len(data))
		res[0] = 2
		copy(res[1:], data)
		return res, nil

	default:
		data, err := h.jsonCodec.Encode(obj)
		if err != nil {
			return nil, err
		}
		// Prefix 0x00 indicates JSON
		res := make([]byte, 1+len(data))
		res[0] = 0
		copy(res[1:], data)
		return res, nil
	}
}

func (h *HybridCodec) Decode(data []byte, into runtime.Object) error {
	if len(data) == 0 {
		return fmt.Errorf("cannot decode empty payload")
	}

	format := data[0]
	payload := data[1:]

	switch format {
	case 1:
		cm, ok := into.(*corev1.ConfigMap)
		if !ok {
			return fmt.Errorf("target object is %T, but data is encoded as ConfigMap protobuf", into)
		}
		return cm.Unmarshal(payload)

	case 2:
		secret, ok := into.(*corev1.Secret)
		if !ok {
			return fmt.Errorf("target object is %T, but data is encoded as Secret protobuf", into)
		}
		return secret.Unmarshal(payload)

	case 0:
		return h.jsonCodec.Decode(payload, into)

	default:
		// Backward compatibility for raw JSON without format prefix
		return h.jsonCodec.Decode(data, into)
	}
}
