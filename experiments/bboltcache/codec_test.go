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

func TestJSONCodec_StripsManagedFields(t *testing.T) {
	codec := NewJSONCodec()

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
	if len(decoded.ManagedFields) != 0 {
		t.Fatalf("expected managedFields to be stripped, got %v", decoded.ManagedFields)
	}
}

func TestJSONCodec_SecretFidelity(t *testing.T) {
	codec := NewJSONCodec()

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
