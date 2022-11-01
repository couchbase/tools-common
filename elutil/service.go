// Package elutil provides a client/service which allows asynchronously reporting events to the '/_event' endpoint
// provided by 'ns_server'.
package elutil

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"

	"github.com/couchbase/tools-common/aprov"
	"github.com/couchbase/tools-common/hofp"
	"github.com/couchbase/tools-common/log"
	"github.com/couchbase/tools-common/maths"
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
	ReqResLogLevel log.Level

	// The number of goroutines to create for reporting events.
	Dispatchers int

	// MaxBufferedEventsPerDispatcher directly maps to the 'hofp.BufferMultiplier' value, and dictates the number of events
	// that can be buffered per-goroutine.
	MaxBufferedEventsPerDispatcher int

	// Logger is the passed Logger struct that implements the Log method for logger the user wants to use.
	Logger log.Logger
}

// Service exposes an interface to report events to the local 'ns_server' instance.
type Service struct {
	pool   *hofp.Pool
	client *Client
	logger log.WrappedLogger
}

// NewService creates a new service using the given options.
func NewService(options ServiceOptions) (*Service, error) {
	client, err := NewClient(options)
	if err != nil {
		return nil, err
	}

	pool := hofp.NewPool(hofp.Options{
		Size:             maths.Max(1, options.Dispatchers),
		BufferMultiplier: maths.Max(MaxBufferedEventsPerDispatcher, options.MaxBufferedEventsPerDispatcher),
		LogPrefix:        "(elutil)", // Should be unused, but set for clarity
	})

	return &Service{pool: pool, client: client, logger: log.NewWrappedLogger(options.Logger)}, nil
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

		s.logger.Errorf("(elutil) Failed to report event '%#v' due to error '%s'", event, err)

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
