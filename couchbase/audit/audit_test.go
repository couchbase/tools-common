package audit

import (
	"context"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	cbaudit "github.com/couchbase/goutils/go-cbaudit"
)

type FakeConn struct {
	Local  net.Addr
	Remote net.Addr
}

func (f *FakeConn) Read(_ []byte) (n int, err error)   { return 0, nil }
func (f *FakeConn) Write(_ []byte) (n int, err error)  { return 0, nil }
func (f *FakeConn) Close() error                       { return nil }
func (f *FakeConn) LocalAddr() net.Addr                { return f.Local }
func (f *FakeConn) RemoteAddr() net.Addr               { return f.Remote }
func (f *FakeConn) SetDeadline(_ time.Time) error      { return nil }
func (f *FakeConn) SetReadDeadline(_ time.Time) error  { return nil }
func (f *FakeConn) SetWriteDeadline(_ time.Time) error { return nil }

func parseAddr(s string) net.Addr {
	return net.TCPAddrFromAddrPort(netip.MustParseAddrPort(s))
}

func TestNewHTTPEvent(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		conn     *FakeConn
		expected *HTTPEvent
		err      bool
	}{
		{name: "NoConn", err: true},
		{
			name: "IPv4Addrs",
			conn: &FakeConn{
				Local:  parseAddr("127.0.0.1:1470"),
				Remote: parseAddr("127.0.0.1:9999"),
			},
			expected: &HTTPEvent{
				URL: "/api/v1/foo",
				LocalRemoteIPs: LocalRemoteIPs{
					Local:  &IPAndPort{IP: "127.0.0.1", Port: 1470},
					Remote: &IPAndPort{IP: "127.0.0.1", Port: 9999},
				},
			},
		},
		{
			name: "IPv6Addrs",
			conn: &FakeConn{
				Local:  parseAddr("[::1]:1470"),
				Remote: parseAddr("[2001:db8:3333:4444:5555:6666:7777:8888]:9999"),
			},
			expected: &HTTPEvent{
				URL: "/api/v1/foo",
				LocalRemoteIPs: LocalRemoteIPs{
					Local:  &IPAndPort{IP: "::1", Port: 1470},
					Remote: &IPAndPort{IP: "2001:db8:3333:4444:5555:6666:7777:8888", Port: 9999},
				},
			},
		},
	}

	u, err := url.Parse("http://localhost:7300/api/v1/foo")
	require.NoError(t, err)

	r := &http.Request{URL: u}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			if test.conn != nil {
				ctx = context.WithValue(ctx, ConnContextKey, test.conn)
			}

			req := r.Clone(ctx)

			evt, err := NewHTTPEvent(req)
			if test.err {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			evt.Timestamp = ""
			evt.RealUserid = cbaudit.RealUserId{}
			require.Equal(t, test.expected, evt)
		})
	}
}
