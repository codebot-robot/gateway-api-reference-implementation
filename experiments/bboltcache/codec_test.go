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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestHybridCodec_ProtobufAndNoSideEffects(t *testing.T) {
	codec := NewHybridCodec()

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cm",
			Namespace: "default",
			ManagedFields: []metav1.ManagedFieldsEntry{
				{
					Manager:   "kubectl",
					Operation: metav1.ManagedFieldsOperationApply,
				},
			},
		},
		Data: map[string]string{
			"key": "value",
		},
	}

	encoded, err := codec.Encode(cm)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	// Verify it encoded using Protobuf (formatProtobuf = 0x01)
	if len(encoded) == 0 || encoded[0] != formatProtobuf {
		t.Fatalf("expected formatProtobuf (0x01), got %x", encoded[0])
	}

	// CRITICAL CHECK: Verify the original object passed to Encode was NOT mutated!
	if len(cm.ManagedFields) != 1 {
		t.Fatalf("side-effect detected: original object managedFields was mutated! len=%d", len(cm.ManagedFields))
	}

	decoded := &corev1.ConfigMap{}
	if err := codec.Decode(encoded, decoded); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if decoded.Name != "test-cm" || decoded.Namespace != "default" {
		t.Fatalf("metadata mismatch: got %s/%s", decoded.Namespace, decoded.Name)
	}
	if decoded.Data["key"] != "value" {
		t.Fatalf("data mismatch: got %v", decoded.Data)
	}
	// The decoded object should have managedFields stripped
	if len(decoded.ManagedFields) != 0 {
		t.Fatalf("expected managedFields to be stripped in stored object, got %v", decoded.ManagedFields)
	}
}

func TestHybridCodec_SecretFidelity(t *testing.T) {
	codec := NewHybridCodec()

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tls-cert",
			Namespace: "prod",
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       []byte("dummy-cert-data"),
			corev1.TLSPrivateKeyKey: []byte("dummy-key-data"),
		},
	}

	encoded, err := codec.Encode(sec)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	if len(encoded) == 0 || encoded[0] != formatProtobuf {
		t.Fatalf("expected formatProtobuf for Secret, got %x", encoded[0])
	}

	decoded := &corev1.Secret{}
	if err := codec.Decode(encoded, decoded); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if decoded.Type != corev1.SecretTypeTLS {
		t.Fatalf("secret type mismatch: got %s", decoded.Type)
	}
	if string(decoded.Data[corev1.TLSCertKey]) != "dummy-cert-data" {
		t.Fatalf("cert mismatch")
	}
	if string(decoded.Data[corev1.TLSPrivateKeyKey]) != "dummy-key-data" {
		t.Fatalf("key mismatch")
	}
}

func TestTransformFunc(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test",
			ManagedFields: []metav1.ManagedFieldsEntry{
				{Manager: "test"},
			},
		},
	}

	transformed, err := TransformFunc(cm)
	if err != nil {
		t.Fatalf("TransformFunc failed: %v", err)
	}
	resCM := transformed.(*corev1.ConfigMap)
	if len(resCM.ManagedFields) != 0 {
		t.Fatalf("expected managedFields to be stripped by TransformFunc")
	}
}
