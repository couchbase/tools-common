package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// ProtoFlag is a type for bitflags representing what protocols to support.
type ProtoFlag uint8

const (
	ProtoFlagHTTP ProtoFlag = 1 << iota
	ProtoFlagHTTPS

	ProtoFlagAll = 0xf
)

type server struct {
	httpServer *http.Server
	wg         sync.WaitGroup
	port       uint16
}

func (s *server) Close() error {
	if s.httpServer == nil {
		return fmt.Errorf("server already closed")
	}

	err := s.httpServer.Close()
	s.wg.Wait()
	s.httpServer = nil

	return err
}

// MultiServer allows one 'http.Handler' to run on multiple protocols and/or listeners. Users can pick any combination
// of http, https, IPv4 and IPv6. Designed to be used for Couchbase service REST API servers where different
// combinations of protocols and IP families must be started based on the server settings.
//
// NOTE: Not thread-safe.
type MultiServer struct {
	handler     http.Handler
	connContext func(ctx context.Context, c net.Conn) context.Context

	httpServer  server
	httpsServer server

	ipv4Mode ListenerMode
	ipv6Mode ListenerMode

	logPrefix string
}

// MultiServerOptions are the options to pass when creating a new 'MultiServer',
type MultiServerOptions struct {
	// Handler is what will handle the requests.
	Handler http.Handler

	// ConnContext specifies a function that modifies the context for a new connection.
	ConnContext func(ctx context.Context, c net.Conn) context.Context

	// LogPrefix is printed at the beginning of each log message the server outputs.
	LogPrefix string

	// IPv4Mode specifies whether to listen on ipv4.
	IPv4Mode ListenerMode

	// IPv6Mode specifies whether to listen on ipv6.
	IPv6Mode ListenerMode

	// HTTPPort is the port to server bare http on.
	HTTPPort uint16

	// HTTPSPort is the port to server https on.
	HTTPSPort uint16
}

// NewMultiServer constructs a 'MultiServer'
func NewMultiServer(opts MultiServerOptions) *MultiServer {
	prefix := opts.LogPrefix
	if prefix != "" {
		prefix += " "
	}

	return &MultiServer{
		handler:     opts.Handler,
		connContext: opts.ConnContext,
		ipv4Mode:    opts.IPv4Mode,
		ipv6Mode:    opts.IPv6Mode,
		httpServer:  server{port: opts.HTTPPort},
		httpsServer: server{port: opts.HTTPSPort},
		logPrefix:   prefix,
	}
}

// Start begins serving the handler on the configured families/protocols. The 'flags' param can be used to specify
// which protocols to accept publicly.
//
// NOTE: If a protocol is not accepted publicly then it is served on the loopback address(es). E.g. passing
// ProtoFlagHTTP in ipv4 mode will start an http server on 0.0.0.0 and an https server on 127.0.0.1.
//
// If the HTTP server succeeds and then the HTTPS fails the user of this method is responsible for closing the HTTP
// server.
func (m *MultiServer) Start(flags ProtoFlag, tlsConfig *tls.Config) error {
	if err := m.start(&m.httpServer, nil, flags&ProtoFlagHTTP == 0); err != nil {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}

	if err := m.start(&m.httpsServer, tlsConfig, flags&ProtoFlagHTTPS == 0); err != nil {
		return fmt.Errorf("failed to start HTTPS server: %w", err)
	}

	return nil
}

func (m *MultiServer) start(server *server, tlsConfig *tls.Config, localOnly bool) error {
	if server.httpServer != nil {
		return nil
	}

	server.httpServer = &http.Server{
		Handler:     m.handler,
		TLSConfig:   tlsConfig,
		ConnContext: m.connContext,
	}

	list4, list6, err := GetListeners(GetListenersOptions{
		IPv4Mode:  m.ipv4Mode,
		IPv6Mode:  m.ipv6Mode,
		Port:      server.port,
		LocalOnly: localOnly,
		LogPrefix: m.logPrefix,
	})
	if err != nil {
		return err
	}

	https := tlsConfig != nil

	for _, list := range []net.Listener{list4, list6} {
		if list == nil {
			continue
		}

		server.wg.Add(1)

		go func(list net.Listener) {
			defer server.wg.Done()

			addr := list.Addr()

			if https {
				list = tls.NewListener(list, tlsConfig)
			}

			slog.Info(m.logPrefix+"HTTP server started on", "address", addr, "https", https)

			if err := server.httpServer.Serve(list); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error(m.logPrefix+"HTTP server stopped", "err", err, "address", addr, "https", https)
			}

			slog.Debug(m.logPrefix+"HTTP server closed", "address", addr, "https", https)
		}(list)
	}

	return nil
}

// Stop stops the servers for the protocols specified by 'flags'.
func (m *MultiServer) Stop(flags ProtoFlag) {
	var (
		wg       sync.WaitGroup
		start    = time.Now()
		shutdown = func(httpServer *server, https bool) {
			wg.Add(1)
			go func() {
				defer wg.Done()

				slog.Info(m.logPrefix+"Shutting down http server", "https", https)
				httpServer.Close()
			}()
		}
	)

	defer func() {
		slog.Info(m.logPrefix+"HTTP servers shut down", "dur", time.Since(start))
	}()

	if flags&ProtoFlagHTTP != 0 {
		shutdown(&m.httpServer, false)
	}

	if flags&ProtoFlagHTTPS != 0 {
		shutdown(&m.httpsServer, true)
	}

	wg.Wait()
}
