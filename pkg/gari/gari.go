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

package gari

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/gke-labs/gateway-api-reference-implementation/pkg/controller"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/proxy"
	"github.com/gke-labs/gateway-api-reference-implementation/pkg/state"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1alpha2 "sigs.k8s.io/gateway-api/apis/v1alpha2"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

var setupLog = ctrl.Log.WithName("gari")

// AddressProvider returns the addresses to report in Gateway.status.addresses.
// An empty result means the address is not assigned yet.
type AddressProvider = controller.AddressProvider

// AddressWatcher is an optional interface an AddressProvider can implement
// to register custom watches with the Gateway controller.
type AddressWatcher = controller.AddressWatcher

// DefaultScheme creates a new runtime.Scheme initialized with client-go and Gateway API schemes.
func DefaultScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(gatewayv1.AddToScheme(scheme))
	utilruntime.Must(gatewayv1beta1.AddToScheme(scheme))
	utilruntime.Must(gatewayv1alpha2.AddToScheme(scheme))
	return scheme
}

// Options contains configuration for running GARI.
type Options struct {
	// RestConfig is the Kubernetes client REST configuration.
	// If nil and Manager is nil, ctrl.GetConfig() will be called.
	RestConfig *rest.Config

	// Scheme is the runtime.Scheme. If nil, DefaultScheme() is used.
	Scheme *runtime.Scheme

	// ControllerName is the GatewayClass controller name managed by this instance.
	// Defaults to controller.DefaultControllerName.
	ControllerName string

	// MetricsAddr is the address the metrics endpoint binds to.
	// Defaults to ":8080". Set to "0" or empty to disable metrics.
	MetricsAddr string

	// HealthProbeBindAddress is the address the health probe endpoint binds to.
	// Defaults to ":8081". Set to "0" or empty to disable.
	HealthProbeBindAddress string

	// ProxyAddr is the address the HTTP proxy server binds to.
	// Defaults to ":8000". Ignored if HTTPListener is provided.
	ProxyAddr string

	// ProxyHTTPSAddr is the address the HTTPS proxy server binds to.
	// Defaults to ":8443". Ignored if HTTPSListener is provided.
	ProxyHTTPSAddr string

	// ProxyHTTP3Addr is the UDP address the HTTP/3 proxy server binds to.
	// Defaults to "" (disabled). Ignored if HTTP3PacketConn is provided.
	ProxyHTTP3Addr string

	// ProxyHTTP3AdvertisedPort is the port advertised in Alt-Svc headers on HTTPS responses.
	// If 0, the port is inferred from the HTTP/3 listener address.
	ProxyHTTP3AdvertisedPort int

	// HTTP3PacketConn is an optional custom net.PacketConn for the HTTP/3 proxy server.
	// If set, ProxyHTTP3Addr is ignored.
	HTTP3PacketConn net.PacketConn

	// HTTP3QUICConfig is an optional custom QUIC configuration for the HTTP/3 proxy server.
	HTTP3QUICConfig *quic.Config

	// EnableH2C enables HTTP/2 Cleartext (H2C) support on the HTTP proxy server.
	EnableH2C bool

	// LeaderElection enables leader election for the controller manager.
	LeaderElection bool

	// LeaderElectionID is the ID used for leader election.
	// Defaults to "gateway-api-reference-implementation".
	LeaderElectionID string

	// HTTPListener is an optional custom net.Listener for the HTTP proxy server.
	// If set, ProxyAddr is ignored.
	HTTPListener net.Listener

	// HTTPSListener is an optional custom net.Listener for the HTTPS proxy server.
	// If set, ProxyHTTPSAddr is ignored.
	HTTPSListener net.Listener

	// DefaultCertificate is an optional default fallback TLS certificate for the HTTPS proxy.
	// If nil, a self-signed certificate will be automatically generated.
	DefaultCertificate *tls.Certificate

	// OnGatewaysUpdate is an optional hook invoked whenever configuration is pushed
	// to the proxy with the currently resolved Gateways.
	OnGatewaysUpdate func(gateways []*gatewayv1.Gateway)

	// AddressProvider is an optional hook that returns addresses to report in Gateway.status.addresses.
	// If nil or returns no addresses, Gateways report Programmed=False with reason AddressNotAssigned.
	AddressProvider AddressProvider

	// GatewayFilter is an optional filter func restricting which Gateways this instance serves.
	// If set, only Gateways for which GatewayFilter returns true are reconciled and served.
	GatewayFilter func(gw *gatewayv1.Gateway) bool

	// Manager is an optional controller-runtime manager. If provided, New will
	// register reconcilers with it instead of creating a new manager.
	Manager ctrl.Manager
}

// DefaultOptions returns standard default options for GARI.
func DefaultOptions() Options {
	return Options{
		ControllerName:         controller.DefaultControllerName,
		MetricsAddr:            ":8080",
		HealthProbeBindAddress: ":8081",
		ProxyAddr:              ":8000",
		ProxyHTTPSAddr:         ":8443",
		LeaderElectionID:       "gateway-api-reference-implementation",
	}
}

func (o *Options) complete() error {
	if o.ControllerName == "" {
		o.ControllerName = controller.DefaultControllerName
	}
	if o.Scheme == nil {
		o.Scheme = DefaultScheme()
	}
	if o.LeaderElectionID == "" {
		o.LeaderElectionID = "gateway-api-reference-implementation"
	}
	return nil
}

// Server encapsulates GARI components: state, proxy, manager, and proxy servers.
type Server struct {
	opts    Options
	manager ctrl.Manager
	state   *state.State
	proxy   *proxy.Proxy
}

// State returns the internal state store.
func (s *Server) State() *state.State {
	return s.state
}

// Proxy returns the internal HTTP proxy.
func (s *Server) Proxy() *proxy.Proxy {
	return s.proxy
}

// Manager returns the controller-runtime Manager, if one was configured or created.
func (s *Server) Manager() ctrl.Manager {
	return s.manager
}

// Options returns the server configuration options.
func (s *Server) Options() Options {
	return s.opts
}

// New creates and initializes a new GARI Server.
// If opts.Manager is nil, a controller-runtime Manager is created using opts.RestConfig and registered with GARI reconcilers.
func New(opts Options) (*Server, error) {
	if err := opts.complete(); err != nil {
		return nil, err
	}

	st := state.NewState()
	p := proxy.NewProxy()

	mgr := opts.Manager
	if mgr == nil {
		cfg := opts.RestConfig
		if cfg == nil {
			var err error
			cfg, err = ctrl.GetConfig()
			if err != nil {
				return nil, fmt.Errorf("unable to get kubeconfig: %w", err)
			}
		}

		var err error
		mgr, err = ctrl.NewManager(cfg, ctrl.Options{
			Scheme: opts.Scheme,
			Metrics: metricsserver.Options{
				BindAddress: opts.MetricsAddr,
			},
			WebhookServer: webhook.NewServer(webhook.Options{
				Port: 9443,
			}),
			HealthProbeBindAddress: opts.HealthProbeBindAddress,
			LeaderElection:         opts.LeaderElection,
			LeaderElectionID:       opts.LeaderElectionID,
		})
		if err != nil {
			return nil, fmt.Errorf("unable to start manager: %w", err)
		}
	}

	s := &Server{
		opts:    opts,
		manager: mgr,
		state:   st,
		proxy:   p,
	}

	if err := s.SetupWithManager(mgr); err != nil {
		return nil, err
	}

	return s, nil
}

// NewWithManager initializes a GARI Server using an existing controller-runtime Manager.
func NewWithManager(mgr ctrl.Manager, opts Options) (*Server, error) {
	opts.Manager = mgr
	return New(opts)
}

// SetupWithManager registers all GARI reconcilers with the given Manager.
func (s *Server) SetupWithManager(mgr ctrl.Manager) error {
	return controller.RegisterReconcilers(mgr, s.state, s.proxy, controller.ReconcilerOptions{
		ControllerName:   s.opts.ControllerName,
		OnGatewaysUpdate: s.opts.OnGatewaysUpdate,
		AddressProvider:  s.opts.AddressProvider,
		GatewayFilter:    s.opts.GatewayFilter,
	})
}

func (s *Server) initDefaultCertificate() error {
	if s.opts.DefaultCertificate != nil {
		s.proxy.SetDefaultCertificate(s.opts.DefaultCertificate)
		return nil
	}
	cert, err := GenerateSelfSignedCert()
	if err != nil {
		return fmt.Errorf("failed to generate self-signed cert: %w", err)
	}
	s.proxy.SetDefaultCertificate(&cert)
	return nil
}

func (s *Server) newTLSConfig() *tls.Config {
	return &tls.Config{
		GetCertificate: s.proxy.GetCertificate,
	}
}

type altSvcResponseWriter struct {
	http.ResponseWriter
	altSvc        string
	writtenHeader bool
}

func (w *altSvcResponseWriter) WriteHeader(statusCode int) {
	if !w.writtenHeader {
		w.Header().Set("Alt-Svc", w.altSvc)
		w.writtenHeader = true
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *altSvcResponseWriter) Write(b []byte) (int, error) {
	if !w.writtenHeader {
		w.Header().Set("Alt-Svc", w.altSvc)
		w.writtenHeader = true
	}
	return w.ResponseWriter.Write(b)
}

func (w *altSvcResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *altSvcResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *altSvcResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := w.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, errors.New("underlying ResponseWriter does not implement http.Hijacker")
}

// StartProxyServers starts the HTTP and HTTPS proxy servers and blocks until ctx is canceled.
func (s *Server) StartProxyServers(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	hasHTTPS := s.opts.HTTPSListener != nil || s.opts.ProxyHTTPSAddr != ""
	hasHTTP3 := s.opts.HTTP3PacketConn != nil || s.opts.ProxyHTTP3Addr != ""
	if hasHTTPS || hasHTTP3 {
		if err := s.initDefaultCertificate(); err != nil {
			return err
		}
	}

	// HTTP Proxy Server
	if s.opts.HTTPListener != nil || s.opts.ProxyAddr != "" {
		g.Go(func() error {
			var lis net.Listener
			if s.opts.HTTPListener != nil {
				lis = s.opts.HTTPListener
			} else {
				var err error
				lis, err = net.Listen("tcp", s.opts.ProxyAddr)
				if err != nil {
					return fmt.Errorf("failed to listen on HTTP proxy address %q: %w", s.opts.ProxyAddr, err)
				}
			}

			setupLog.Info("starting proxy server", "addr", lis.Addr().String())
			var handler http.Handler = s.proxy
			if s.opts.EnableH2C {
				h2s := &http2.Server{}
				handler = h2c.NewHandler(s.proxy, h2s)
			}
			srv := &http.Server{
				Handler: handler,
			}
			go func() {
				<-ctx.Done()
				setupLog.Info("shutting down proxy server")
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = srv.Shutdown(shutdownCtx)
			}()
			if err := srv.Serve(lis); err != nil && err != http.ErrServerClosed {
				return fmt.Errorf("proxy server failed: %w", err)
			}
			return nil
		})
	}

	var h3Conn net.PacketConn
	var advertisedH3Port int

	if hasHTTP3 {
		if s.opts.HTTP3PacketConn != nil {
			h3Conn = s.opts.HTTP3PacketConn
		} else {
			var err error
			h3Conn, err = net.ListenPacket("udp", s.opts.ProxyHTTP3Addr)
			if err != nil {
				return fmt.Errorf("failed to listen on HTTP/3 proxy address %q: %w", s.opts.ProxyHTTP3Addr, err)
			}
		}

		advertisedH3Port = s.opts.ProxyHTTP3AdvertisedPort
		if advertisedH3Port == 0 && h3Conn != nil {
			if udpAddr, ok := h3Conn.LocalAddr().(*net.UDPAddr); ok {
				advertisedH3Port = udpAddr.Port
			} else {
				_, portStr, err := net.SplitHostPort(h3Conn.LocalAddr().String())
				if err == nil {
					if p, err := strconv.Atoi(portStr); err == nil {
						advertisedH3Port = p
					}
				}
			}
		}
	}

	// HTTPS Proxy Server
	if hasHTTPS {
		g.Go(func() error {
			var lis net.Listener
			if s.opts.HTTPSListener != nil {
				lis = s.opts.HTTPSListener
			} else {
				var err error
				lis, err = net.Listen("tcp", s.opts.ProxyHTTPSAddr)
				if err != nil {
					return fmt.Errorf("failed to listen on HTTPS proxy address %q: %w", s.opts.ProxyHTTPSAddr, err)
				}
			}

			setupLog.Info("starting proxy HTTPS server", "addr", lis.Addr().String())

			var handler http.Handler = s.proxy
			if hasHTTP3 && advertisedH3Port > 0 {
				altSvcVal := fmt.Sprintf(`h3=":%d"; ma=86400`, advertisedH3Port)
				handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					s.proxy.ServeHTTP(&altSvcResponseWriter{
						ResponseWriter: w,
						altSvc:         altSvcVal,
					}, r)
				})
			}

			tlsConfig := s.newTLSConfig()
			srv := &http.Server{
				Handler:   handler,
				TLSConfig: tlsConfig,
			}
			if err := http2.ConfigureServer(srv, &http2.Server{}); err != nil {
				return fmt.Errorf("failed to configure HTTP/2 on HTTPS proxy: %w", err)
			}
			tlsLis := tls.NewListener(lis, tlsConfig)
			go func() {
				<-ctx.Done()
				setupLog.Info("shutting down proxy HTTPS server")
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = srv.Shutdown(shutdownCtx)
			}()
			if err := srv.Serve(tlsLis); err != nil && err != http.ErrServerClosed {
				return fmt.Errorf("proxy HTTPS server failed: %w", err)
			}
			return nil
		})
	}

	// HTTP/3 Proxy Server
	if hasHTTP3 && h3Conn != nil {
		g.Go(func() error {
			setupLog.Info("starting proxy HTTP/3 server", "addr", h3Conn.LocalAddr().String())

			tlsConfig := s.newTLSConfig()
			h3Server := &http3.Server{
				Handler:    s.proxy,
				TLSConfig:  tlsConfig,
				Port:       advertisedH3Port,
				QUICConfig: s.opts.HTTP3QUICConfig,
			}

			go func() {
				<-ctx.Done()
				setupLog.Info("shutting down proxy HTTP/3 server")
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = h3Server.Shutdown(shutdownCtx)
				_ = h3Conn.Close()
			}()

			if err := h3Server.Serve(h3Conn); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, quic.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
				return fmt.Errorf("proxy HTTP/3 server failed: %w", err)
			}
			return nil
		})
	}

	return g.Wait()
}

// Start starts both the proxy servers and the controller manager, running until ctx is canceled.
func (s *Server) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return s.StartProxyServers(ctx)
	})

	if s.manager != nil {
		g.Go(func() error {
			setupLog.Info("starting manager")
			if err := s.manager.Start(ctx); err != nil {
				return fmt.Errorf("problem running manager: %w", err)
			}
			return nil
		})
	}

	return g.Wait()
}

// Run creates a new Server with opts and runs it until ctx is canceled.
func Run(ctx context.Context, opts Options) error {
	s, err := New(opts)
	if err != nil {
		return err
	}
	return s.Start(ctx)
}
