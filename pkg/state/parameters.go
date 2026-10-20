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

package state

import (
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/yaml"
)

// Supported parameter keys in ConfigMap.
const (
	ParameterKeyServiceType           = "serviceType"
	ParameterKeyExternalTrafficPolicy = "externalTrafficPolicy"
	ParameterKeyServiceAnnotations    = "serviceAnnotations"
)

// Parameters represents configuration parsed from a ConfigMap for Gateway/GatewayClass.
type Parameters struct {
	ServiceType           corev1.ServiceType
	ExternalTrafficPolicy corev1.ServiceExternalTrafficPolicy
	ServiceAnnotations    map[string]string
}

// DefaultParameters returns default parameters (LoadBalancer, Cluster).
func DefaultParameters() *Parameters {
	return &Parameters{
		ServiceType:           corev1.ServiceTypeLoadBalancer,
		ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyCluster,
		ServiceAnnotations:    make(map[string]string),
	}
}

// ParseConfigMapParameters parses and validates parameters from a ConfigMap.
// Unknown keys or invalid values return an error.
func ParseConfigMapParameters(cm *corev1.ConfigMap) (*Parameters, error) {
	if cm == nil {
		return &Parameters{
			ServiceAnnotations: make(map[string]string),
		}, nil
	}

	if len(cm.BinaryData) > 0 {
		for k := range cm.BinaryData {
			return nil, fmt.Errorf("unknown key %q in ConfigMap binaryData", k)
		}
	}

	for k := range cm.Data {
		switch k {
		case ParameterKeyServiceType, ParameterKeyExternalTrafficPolicy, ParameterKeyServiceAnnotations:
		default:
			return nil, fmt.Errorf("unknown key %q in ConfigMap", k)
		}
	}

	p := &Parameters{
		ServiceAnnotations: make(map[string]string),
	}

	if val, ok := cm.Data[ParameterKeyServiceType]; ok && val != "" {
		switch val {
		case string(corev1.ServiceTypeLoadBalancer):
			p.ServiceType = corev1.ServiceTypeLoadBalancer
		case string(corev1.ServiceTypeClusterIP):
			p.ServiceType = corev1.ServiceTypeClusterIP
		default:
			return nil, fmt.Errorf("invalid serviceType %q: must be LoadBalancer or ClusterIP", val)
		}
	}

	if val, ok := cm.Data[ParameterKeyExternalTrafficPolicy]; ok && val != "" {
		switch val {
		case string(corev1.ServiceExternalTrafficPolicyCluster):
			p.ExternalTrafficPolicy = corev1.ServiceExternalTrafficPolicyCluster
		case string(corev1.ServiceExternalTrafficPolicyLocal):
			p.ExternalTrafficPolicy = corev1.ServiceExternalTrafficPolicyLocal
		default:
			return nil, fmt.Errorf("invalid externalTrafficPolicy %q: must be Cluster or Local", val)
		}
	}

	if val, ok := cm.Data[ParameterKeyServiceAnnotations]; ok && val != "" {
		var raw any
		if err := yaml.Unmarshal([]byte(val), &raw); err != nil {
			return nil, fmt.Errorf("invalid serviceAnnotations: %w", err)
		}
		if raw != nil {
			rawMap, ok := raw.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("invalid serviceAnnotations: expected YAML/JSON map")
			}
			for k, v := range rawMap {
				strVal, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("invalid serviceAnnotations: value for key %q must be a string", k)
				}
				p.ServiceAnnotations[k] = strVal
			}
		}
	}

	return p, nil
}

// ValidateGatewayClassParametersRef validates a GatewayClass's ParametersReference.
func ValidateGatewayClassParametersRef(ref *gatewayv1.ParametersReference, configMaps map[types.NamespacedName]*corev1.ConfigMap) (*Parameters, error) {
	if ref == nil {
		return nil, nil
	}
	if ref.Group != "" {
		return nil, fmt.Errorf("unsupported group %q: only core group (\"\") is supported", ref.Group)
	}
	if ref.Kind != "ConfigMap" {
		return nil, fmt.Errorf("unsupported kind %q: only ConfigMap is supported", ref.Kind)
	}
	if ref.Namespace == nil || *ref.Namespace == "" {
		return nil, fmt.Errorf("namespace is required for ConfigMap parametersRef")
	}
	if ref.Name == "" {
		return nil, fmt.Errorf("name is required for ConfigMap parametersRef")
	}
	cmKey := types.NamespacedName{Namespace: string(*ref.Namespace), Name: string(ref.Name)}
	var cm *corev1.ConfigMap
	if configMaps != nil {
		cm = configMaps[cmKey]
	}
	if cm == nil {
		return nil, fmt.Errorf("ConfigMap %s/%s not found", cmKey.Namespace, cmKey.Name)
	}
	return ParseConfigMapParameters(cm)
}

// ValidateGatewayParametersRef validates a Gateway's infrastructure LocalParametersReference.
func ValidateGatewayParametersRef(ref *gatewayv1.LocalParametersReference, gwNamespace string, configMaps map[types.NamespacedName]*corev1.ConfigMap) (*Parameters, error) {
	if ref == nil {
		return nil, nil
	}
	if ref.Group != "" {
		return nil, fmt.Errorf("unsupported group %q: only core group (\"\") is supported", ref.Group)
	}
	if ref.Kind != "ConfigMap" {
		return nil, fmt.Errorf("unsupported kind %q: only ConfigMap is supported", ref.Kind)
	}
	if ref.Name == "" {
		return nil, fmt.Errorf("name is required for ConfigMap parametersRef")
	}
	cmKey := types.NamespacedName{Namespace: gwNamespace, Name: string(ref.Name)}
	var cm *corev1.ConfigMap
	if configMaps != nil {
		cm = configMaps[cmKey]
	}
	if cm == nil {
		return nil, fmt.Errorf("ConfigMap %s/%s not found", cmKey.Namespace, cmKey.Name)
	}
	return ParseConfigMapParameters(cm)
}

// MergeParameters merges GatewayClass parameters and Gateway parameters.
// Gateway values override GatewayClass values.
// Defaults:
// - ServiceType: LoadBalancer
// - ExternalTrafficPolicy: Cluster
func MergeParameters(gcParams, gwParams *Parameters) *Parameters {
	res := &Parameters{
		ServiceType:           corev1.ServiceTypeLoadBalancer,
		ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyCluster,
		ServiceAnnotations:    make(map[string]string),
	}

	if gcParams != nil {
		if gcParams.ServiceType != "" {
			res.ServiceType = gcParams.ServiceType
		}
		if gcParams.ExternalTrafficPolicy != "" {
			res.ExternalTrafficPolicy = gcParams.ExternalTrafficPolicy
		}
		for k, v := range gcParams.ServiceAnnotations {
			res.ServiceAnnotations[k] = v
		}
	}

	if gwParams != nil {
		if gwParams.ServiceType != "" {
			res.ServiceType = gwParams.ServiceType
		}
		if gwParams.ExternalTrafficPolicy != "" {
			res.ExternalTrafficPolicy = gwParams.ExternalTrafficPolicy
		}
		for k, v := range gwParams.ServiceAnnotations {
			res.ServiceAnnotations[k] = v
		}
	}

	return res
}

// ParametersEqual returns true if two Parameters structs are equal.
func ParametersEqual(a, b *Parameters) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.ServiceType != b.ServiceType {
		return false
	}
	if a.ExternalTrafficPolicy != b.ExternalTrafficPolicy {
		return false
	}
	return maps.Equal(a.ServiceAnnotations, b.ServiceAnnotations)
}
