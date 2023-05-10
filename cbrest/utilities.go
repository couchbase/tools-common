package cbrest

import (
	"github.com/couchbase/tools-common/netutil"
	"github.com/couchbase/tools-common/ptrutil"
)

// newDefaultHTTPTimeouts returns the default REST HTTP client timeouts.
func newDefaultHTTPTimeouts() netutil.HTTPTimeouts {
	return netutil.HTTPTimeouts{
		Dialer:                  ptrutil.ToPtr(DefaultDialerTimeout),
		KeepAlive:               ptrutil.ToPtr(DefaultDialerKeepAlive),
		TransportIdleConn:       ptrutil.ToPtr(DefaultTransportIdleConnTimeout),
		TransportContinue:       ptrutil.ToPtr(DefaultTransportContinueTimeout),
		TransportResponseHeader: ptrutil.ToPtr(DefaultResponseHeaderTimeout),
		TransportTLSHandshake:   ptrutil.ToPtr(DefaultTLSHandshakeTimeout),
	}
}
