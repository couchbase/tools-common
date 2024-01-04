// Package evtlog provides a client/service which allows asynchronously reporting events to the '/_event' endpoint
// provided by 'ns_server'.
package evtlog

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"

	aprov "github.com/couchbase/tools-common/auth/v2/provider"
	"github.com/couchbase/tools-common/sync/v2/hofp"
)

const (
	// MaxEncodedLength is the maximum size of the JSON encoded event.
	MaxEncodedLength = 3 * 1024

	// MaxBufferedEventsPerDispatcher is the default number of events (and minimum) number of events that can be buffered
	// per-goroutine.
	MaxBufferedEventsPerDispatcher = 8
)

// ServiceOptions encapsulates the options available when creating a new event log service.
type ServiceOptions struct {
	ManagementPort uint16
	Provider       aprov.Provider
	TLSConfig      *tls.Config

	// ReqResLogLevel is the level at which HTTP requests/responses will be logged; this directly maps to the 'cbrest'
	// client value.
	ReqResLogLevel slog.Level

	// The number of goroutines to create for reporting events.
	Dispatchers int

	// MaxBufferedEventsPerDispatcher directly maps to the 'hofp.BufferMultiplier' value, and dictates the number of events
	// that can be buffered per-goroutine.
	MaxBufferedEventsPerDispatcher int

	// Logger is the passed Logger struct that implements the Log method for logger the user wants to use.
	Logger *slog.Logger
}

// defaults fills any missing attributes to a sane default.
func (s *ServiceOptions) defaults() {
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
}

// Service exposes an interface to report events to the local 'ns_server' instance.
type Service struct {
	pool   *hofp.Pool
	client *Client
	logger *slog.Logger
}

// NewService creates a new service using the given options.
func NewService(options ServiceOptions) (*Service, error) {
	// Fill out any missing fields with the sane defaults
	options.defaults()

	client, err := NewClient(options)
	if err != nil {
		return nil, err
	}

	pool := hofp.NewPool(hofp.Options{
		Size:             max(1, options.Dispatchers),
		BufferMultiplier: max(MaxBufferedEventsPerDispatcher, options.MaxBufferedEventsPerDispatcher),
	})

	service := Service{
		pool:   pool,
		client: client,
		logger: options.Logger,
	}

	return &service, nil
}

// Report the given event asynchronously, logging event/error if we fail to do so.
//
// NOTE: To "receive" these logged errors, a 'Logger' implementation should be provided to 'log.SetLogger'.
func (s *Service) Report(event Event) {
	s.pool.Queue(func(ctx context.Context) error { //nolint:errcheck
		err := s.report(ctx, event)
		if err == nil {
			return nil
		}

		s.logger.Error("failed to report event", "event", event, "error", err)

		return nil
	})
}

// report the given event synchronously.
func (s *Service) report(ctx context.Context, event Event) error {
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to encode event: %w", err)
	}

	if len(encoded) > MaxEncodedLength {
		return ErrTooLarge
	}

	return s.client.PostEvent(ctx, encoded)
}

// Close gracefully stops the service, reporting any buffered events then closes the internal pool/client.
//
// NOTE: A service being used after closure has undefined behavior.
func (s *Service) Close() {
	s.pool.Stop() //nolint:errcheck
	s.client.Close()
}
