package server

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func myIP(t *testing.T, v4 bool) string {
	addrs, err := net.InterfaceAddrs()
	require.NoError(t, err)

	for _, addr := range addrs {
		ipnet, ok := addr.(*net.IPNet)
		if !ok || !ipnet.IP.IsGlobalUnicast() || ipnet.IP.IsLoopback() {
			continue
		}

		if v4 && ipnet.IP.To4() == nil {
			continue
		}

		if !v4 && ipnet.IP.To4() != nil {
			continue
		}

		return ipnet.IP.String()
	}

	// Getting here means we don't have an address - presumably because we're asking for a v6 one and haven't got one.
	// We log what addrs we did get for debugging purposes and then skip the test.

	for _, addr := range addrs {
		ipnet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}

		t.Logf("addr %s, IsLoopback=%t IsGlobalUnicast=%t", ipnet.IP, ipnet.IP.IsLoopback(), ipnet.IP.IsGlobalUnicast())
	}

	t.Skip("could not find ip own address")

	return "" // Unreachable
}

func fmtAddr(v6 bool, proto, ip string, port uint) string {
	host := ip
	if v6 {
		host = fmt.Sprintf("[%s]", host)
	}

	return fmt.Sprintf("%s://%s:%d", proto, host, port)
}

func TestServerAllLocal(t *testing.T) {
	cert, err := tls.LoadX509KeyPair("testdata/cert.pem", "testdata/key.pem")
	require.NoError(t, err)

	s := NewMultiServer(MultiServerOptions{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, err := w.Write([]byte("OK"))
			require.NoError(t, err)
		}),
		IPv4Mode:  ListenerModeMust,
		IPv6Mode:  ListenerModeMust,
		HTTPPort:  1470,
		HTTPSPort: 1471,
	})

	require.NoError(t, s.Start(ProtoFlagHTTP|ProtoFlagHTTPS, &tls.Config{Certificates: []tls.Certificate{cert}}))

	cli := http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
		Timeout:   5 * time.Second,
	}

	addrs := []struct {
		addr, name string
	}{
		{addr: "http://127.0.0.1:1470", name: "IPv4HTTP"},
		{addr: "http://[::1]:1470", name: "IPv6HTTP"},
		{addr: "https://127.0.0.1:1471", name: "IPv4HTTPS"},
		{addr: "https://[::1]:1471", name: "IPv6HTTPS"},
	}

	for _, addr := range addrs {
		t.Run(fmt.Sprintf("%sRunning", addr.name), func(t *testing.T) {
			request, err := http.NewRequest(http.MethodGet, addr.addr, nil)
			require.NoError(t, err)

			res, err := cli.Do(request)
			require.NoError(t, err)

			_ = res.Body.Close()
		})
	}

	s.Stop(ProtoFlagHTTP | ProtoFlagHTTPS)

	for _, addr := range addrs {
		t.Run(fmt.Sprintf("%sStopped", addr.name), func(t *testing.T) {
			request, err := http.NewRequest(http.MethodGet, addr.addr, nil)
			require.NoError(t, err)

			_, err = cli.Do(request) //nolint:bodyclose
			require.Error(t, err)
		})
	}
}

func TestServerExhaustive(t *testing.T) {
	cert, err := tls.LoadX509KeyPair("testdata/cert.pem", "testdata/key.pem")
	require.NoError(t, err)

	type modes struct {
		v4 ListenerMode
		v6 ListenerMode
	}

	var (
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}}

		cli = http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
		req = func(addr string) error {
			request, err := http.NewRequest(http.MethodGet, addr, nil)
			if err != nil {
				return err
			}

			res, err := cli.Do(request)
			if res != nil {
				_ = res.Body.Close()
			}

			return err
		}
		reqTest = func(t *testing.T, addr string, succeeds bool) {
			slog.Info("doing request", "addr", addr)

			if !succeeds {
				require.Error(t, req(addr))
				return
			}

			require.NoError(t, req(addr))
		}

		ipv4 = myIP(t, true)
		ipv6 = myIP(t, false)

		allFlags = []ProtoFlag{ProtoFlagHTTP, ProtoFlagHTTPS, ProtoFlagAll}
		allModes = []modes{{v4: ListenerModeMust}, {v6: ListenerModeMust}, {v4: ListenerModeMust, v6: ListenerModeMust}}

		testName = func(f ProtoFlag, m modes) string {
			var s string
			if f == ProtoFlagAll {
				s = "HTTP+S"
			} else if f == ProtoFlagHTTP {
				s = "HTTP"
			} else {
				s = "HTTPS"
			}

			if m.v4 == ListenerModeMust {
				s += "v4"
			}
			if m.v6 == ListenerModeMust {
				s += "v6"
			}

			return s
		}
	)

	for _, flag := range allFlags {
		for _, mode := range allModes {
			t.Run(testName(flag, mode), func(t *testing.T) {
				s := NewMultiServer(MultiServerOptions{
					Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						_, err = w.Write([]byte("OK"))
						require.NoError(t, err)
					}),
					IPv4Mode:  mode.v4,
					IPv6Mode:  mode.v6,
					HTTPPort:  1470,
					HTTPSPort: 1471,
				})

				require.NoError(t, s.Start(flag, tlsConfig))

				reqTest(t, fmtAddr(false, "http", ipv4, 1470), mode.v4 == ListenerModeMust && (flag&ProtoFlagHTTP) != 0)
				reqTest(t, fmtAddr(true, "http", ipv6, 1470), mode.v6 == ListenerModeMust && (flag&ProtoFlagHTTP) != 0)
				reqTest(t, fmtAddr(false, "https", ipv4, 1471), mode.v4 == ListenerModeMust && (flag&ProtoFlagHTTPS) != 0)
				reqTest(t, fmtAddr(true, "https", ipv6, 1471), mode.v6 == ListenerModeMust && (flag&ProtoFlagHTTPS) != 0)

				s.Stop(ProtoFlagAll)

				reqTest(t, fmtAddr(false, "http", ipv4, 1470), false)
				reqTest(t, fmtAddr(true, "http", ipv6, 1470), false)
				reqTest(t, fmtAddr(false, "https", ipv4, 1471), false)
				reqTest(t, fmtAddr(true, "https", ipv6, 1471), false)
			})
		}
	}
}
