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
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// CertificateData holds the serialized DER bytes of a TLS certificate chain and PKCS#8 private key.
type CertificateData struct {
	Certificate [][]byte `json:"certificate"`
	PrivateKey  []byte   `json:"privateKey"`
}

const (
	// LabelManagedBy is the Kubernetes label key indicating the managing controller.
	LabelManagedBy = "app.kubernetes.io/managed-by"

	// ManagedByValue is the label value for resources managed by singlepod provisioning.
	ManagedByValue = "gari-singlepod"

	// LabelAppName is the label key identifying the application component.
	LabelAppName = "app.kubernetes.io/name"

	// AppNameValue is the label value identifying data-plane components.
	AppNameValue = "gari-dataplane"

	// LabelGatewayName is the Gateway API label key for the parent Gateway name.
	LabelGatewayName = "gateway.networking.k8s.io/gateway-name"
)

// DataplaneConfig represents the serialized proxy configuration for a single Gateway.
type DataplaneConfig struct {
	GatewayName      types.NamespacedName       `json:"gatewayName"`
	Listeners        []InternalListener         `json:"listeners"`
	Routes           []InternalRoute            `json:"routes"`
	Certificates     map[string]CertificateData `json:"certificates,omitempty"`     // certID -> CertificateData
	HostCertificates map[string]string          `json:"hostCertificates,omitempty"` // hostname -> certID
	DefaultCertID    string                     `json:"defaultCertID,omitempty"`
}

// DataplanePublisher defines the interface for publishing a Gateway's compiled data-plane configuration.
type DataplanePublisher interface {
	Begin(ctx context.Context) error
	PublishDataplaneConfig(ctx context.Context, gw *gatewayv1.Gateway, config *DataplaneConfig) error
	Commit(ctx context.Context) error
}

func tlsCertToData(cert *tls.Certificate) (CertificateData, error) {
	if cert == nil {
		return CertificateData{}, nil
	}
	pkBytes, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		return CertificateData{}, fmt.Errorf("failed to marshal private key to PKCS#8: %w", err)
	}
	return CertificateData{
		Certificate: cert.Certificate,
		PrivateKey:  pkBytes,
	}, nil
}

func dataToTLSCert(data CertificateData) (*tls.Certificate, error) {
	if len(data.Certificate) == 0 || len(data.PrivateKey) == 0 {
		return nil, nil
	}
	pk, err := x509.ParsePKCS8PrivateKey(data.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PKCS#8 private key: %w", err)
	}
	cert := &tls.Certificate{
		Certificate: data.Certificate,
		PrivateKey:  pk,
	}
	if len(cert.Certificate) > 0 {
		leaf, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return nil, fmt.Errorf("failed to parse leaf certificate: %w", err)
		}
		cert.Leaf = leaf
	}
	return cert, nil
}

// BuildDataplaneConfig constructs a DataplaneConfig from listeners, routes, certificates, and default certificate.
func BuildDataplaneConfig(
	gwKey types.NamespacedName,
	listeners []InternalListener,
	routes []InternalRoute,
	certsMap map[string]*tls.Certificate,
	defaultCert *tls.Certificate,
) (*DataplaneConfig, error) {
	cfg := &DataplaneConfig{
		GatewayName:      gwKey,
		Listeners:        listeners,
		Routes:           routes,
		Certificates:     make(map[string]CertificateData),
		HostCertificates: make(map[string]string),
	}

	certIDMap := make(map[*tls.Certificate]string)
	getCertID := func(c *tls.Certificate) (string, error) {
		if c == nil {
			return "", nil
		}
		if id, ok := certIDMap[c]; ok {
			return id, nil
		}
		data, err := tlsCertToData(c)
		if err != nil {
			return "", err
		}
		var id string
		if len(c.Certificate) > 0 {
			h := sha256.Sum256(c.Certificate[0])
			id = hex.EncodeToString(h[:])
		} else {
			h := sha256.Sum256(data.PrivateKey)
			id = hex.EncodeToString(h[:])
		}
		certIDMap[c] = id
		cfg.Certificates[id] = data
		return id, nil
	}

	if defaultCert != nil {
		id, err := getCertID(defaultCert)
		if err != nil {
			return nil, err
		}
		cfg.DefaultCertID = id
	}

	var hosts []string
	for host := range certsMap {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)

	for _, host := range hosts {
		cert := certsMap[host]
		if cert != nil {
			id, err := getCertID(cert)
			if err != nil {
				return nil, err
			}
			cfg.HostCertificates[host] = id
		}
	}

	return cfg, nil
}

// ExtractCertificates reconstructs the certificates map and default certificate from DataplaneConfig.
func (cfg *DataplaneConfig) ExtractCertificates() (map[string]*tls.Certificate, *tls.Certificate, error) {
	if cfg == nil {
		return nil, nil, nil
	}

	parsedCerts := make(map[string]*tls.Certificate)
	for id, data := range cfg.Certificates {
		tlsCert, err := dataToTLSCert(data)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse certificate %s: %w", id, err)
		}
		parsedCerts[id] = tlsCert
	}

	certsMap := make(map[string]*tls.Certificate)
	for host, id := range cfg.HostCertificates {
		if cert, ok := parsedCerts[id]; ok {
			certsMap[host] = cert
		}
	}

	var defaultCert *tls.Certificate
	if cfg.DefaultCertID != "" {
		defaultCert = parsedCerts[cfg.DefaultCertID]
	}

	return certsMap, defaultCert, nil
}

// BuildGatewayDataplaneConfig builds a DataplaneConfig for a single compiled Gateway.
func BuildGatewayDataplaneConfig(
	cg *CompiledGateway,
	secrets map[types.NamespacedName]*corev1.Secret,
	refValidator ReferenceGrantValidator,
) (*DataplaneConfig, error) {
	if cg == nil || cg.Gateway == nil {
		return nil, nil
	}
	gwKey := types.NamespacedName{Namespace: cg.Gateway.Namespace, Name: cg.Gateway.Name}
	listeners, routes := BuildProxyConfig([]*CompiledGateway{cg})
	certsMap, defaultCert := ExtractCertificates([]*CompiledGateway{cg}, secrets, refValidator)

	return BuildDataplaneConfig(gwKey, listeners, routes, certsMap, defaultCert)
}

// Marshal serializes DataplaneConfig to JSON.
func (cfg *DataplaneConfig) Marshal() ([]byte, error) {
	return json.Marshal(cfg)
}

// UnmarshalDataplaneConfig deserializes DataplaneConfig from JSON.
func UnmarshalDataplaneConfig(data []byte) (*DataplaneConfig, error) {
	var cfg DataplaneConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ProxyConfigUpdater defines the interface needed to apply configuration and certificates to a proxy.
type ProxyConfigUpdater interface {
	UpdateConfig(listeners []InternalListener, routes []InternalRoute)
	UpdateCertificates(certs map[string]*tls.Certificate, defaultCert *tls.Certificate)
}

// ApplyDataplaneConfig deserializes JSON-encoded DataplaneConfig, extracts certificates,
// and applies the configuration and certificates to the proxy updater.
func ApplyDataplaneConfig(updater ProxyConfigUpdater, data []byte) error {
	if updater == nil {
		return nil
	}
	cfg, err := UnmarshalDataplaneConfig(data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal dataplane config: %w", err)
	}
	certsMap, defaultCert, err := cfg.ExtractCertificates()
	if err != nil {
		return fmt.Errorf("failed to extract certificates: %w", err)
	}
	updater.UpdateConfig(cfg.Listeners, cfg.Routes)
	updater.UpdateCertificates(certsMap, defaultCert)
	return nil
}

// InProcessDataplanePublisher is a DataplanePublisher that serializes DataplaneConfig
// to JSON and applies it to an in-process proxy using ApplyDataplaneConfig.
type InProcessDataplanePublisher struct {
	mu      sync.Mutex
	updater ProxyConfigUpdater
	configs map[types.NamespacedName]*DataplaneConfig
	inBatch bool
	dirty   bool
}

// NewInProcessDataplanePublisher creates a new in-process DataplanePublisher for the given proxy updater.
func NewInProcessDataplanePublisher(updater ProxyConfigUpdater) *InProcessDataplanePublisher {
	return &InProcessDataplanePublisher{
		updater: updater,
		configs: make(map[types.NamespacedName]*DataplaneConfig),
	}
}

// Begin starts a batch of config updates. When in a batch, updates to the proxy
// are deferred until Commit is called.
func (p *InProcessDataplanePublisher) Begin(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inBatch = true
	p.dirty = false
	return nil
}

// PublishDataplaneConfig records the config for a Gateway. If inside a Begin/Commit batch,
// the proxy update is deferred until Commit. Otherwise, it is applied immediately.
func (p *InProcessDataplanePublisher) PublishDataplaneConfig(ctx context.Context, gw *gatewayv1.Gateway, config *DataplaneConfig) error {
	if p == nil || p.updater == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	p.dirty = true

	if gw == nil || config == nil {
		if gw != nil {
			delete(p.configs, types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name})
		} else {
			p.configs = make(map[types.NamespacedName]*DataplaneConfig)
		}
	} else {
		gwKey := types.NamespacedName{Namespace: gw.Namespace, Name: gw.Name}
		if p.configs == nil {
			p.configs = make(map[types.NamespacedName]*DataplaneConfig)
		}
		p.configs[gwKey] = config
	}

	if !p.inBatch {
		return p.applyLocked()
	}
	return nil
}

// Commit finalizes the batch and applies the merged configuration to the proxy if modified.
func (p *InProcessDataplanePublisher) Commit(ctx context.Context) error {
	if p == nil || p.updater == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inBatch = false
	if !p.dirty {
		return nil
	}
	p.dirty = false
	return p.applyLocked()
}

func (p *InProcessDataplanePublisher) applyLocked() error {
	if len(p.configs) == 0 {
		emptyData, err := (&DataplaneConfig{}).Marshal()
		if err != nil {
			return err
		}
		return ApplyDataplaneConfig(p.updater, emptyData)
	}

	if len(p.configs) == 1 {
		var singleCfg *DataplaneConfig
		for _, cfg := range p.configs {
			singleCfg = cfg
		}
		data, err := singleCfg.Marshal()
		if err != nil {
			return fmt.Errorf("failed to marshal dataplane config: %w", err)
		}
		return ApplyDataplaneConfig(p.updater, data)
	}

	// Sort Gateway keys for deterministic merging.
	keys := make([]types.NamespacedName, 0, len(p.configs))
	for k := range p.configs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Namespace != keys[j].Namespace {
			return keys[i].Namespace < keys[j].Namespace
		}
		return keys[i].Name < keys[j].Name
	})

	combined := &DataplaneConfig{
		Certificates:     make(map[string]CertificateData),
		HostCertificates: make(map[string]string),
	}
	for _, k := range keys {
		cfg := p.configs[k]
		combined.Listeners = append(combined.Listeners, cfg.Listeners...)
		combined.Routes = append(combined.Routes, cfg.Routes...)
		for certID, certData := range cfg.Certificates {
			combined.Certificates[certID] = certData
		}
		for host, certID := range cfg.HostCertificates {
			combined.HostCertificates[host] = certID
		}
		if combined.DefaultCertID == "" {
			combined.DefaultCertID = cfg.DefaultCertID
		}
	}
	data, err := combined.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal combined dataplane config: %w", err)
	}
	return ApplyDataplaneConfig(p.updater, data)
}

var _ DataplanePublisher = (*InProcessDataplanePublisher)(nil)
