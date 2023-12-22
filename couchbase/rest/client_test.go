package rest

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	aprov "github.com/couchbase/tools-common/auth/v2/provider"
	"github.com/couchbase/tools-common/core/log"
	"github.com/couchbase/tools-common/couchbase/v2/connstr"
	netutil "github.com/couchbase/tools-common/http/util"
	testutil "github.com/couchbase/tools-common/testing/util"

	"github.com/stretchr/testify/require"
)

// provider is the auth provider used in the testing; it should not be modified.
var provider = &aprov.Static{
	UserAgent:   "user-agent",
	Credentials: aprov.Credentials{Username: "username", Password: "password"},
}

// newTestClient returns a client which is boostrapped against the provided cluster.
//
// NOTE: Returns an error because some tests expect bootstrapping to fail.
func newTestClient(cluster *TestCluster, disableCCP bool) (*Client, error) {
	pool := x509.NewCertPool()

	if cluster.Certificate() != nil {
		pool.AddCert(cluster.Certificate())
	}

	return NewClient(ClientOptions{
		ConnectionString: cluster.URL(),
		DisableCCP:       disableCCP,
		Provider:         provider,
		TLSConfig:        &tls.Config{RootCAs: pool},
		Logger:           log.StdoutLogger{},
	})
}

func TestNewClientWithTransportDefaults(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	transport := client.client.Transport.(*http.Transport)

	require.Zero(t, transport.ExpectContinueTimeout)
	require.Equal(t, 10*time.Second, transport.TLSHandshakeTimeout)
	require.Equal(t, 100, transport.MaxIdleConns)
	require.Equal(t, 90*time.Second, transport.IdleConnTimeout)
	require.NotNil(t, transport.DialContext)
	require.Nil(t, transport.Proxy)
	require.NotNil(t, transport.TLSClientConfig)
	require.True(t, transport.ForceAttemptHTTP2)
}

func TestNewClientWithThisNodeOnly(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := NewClient(ClientOptions{
		ConnectionString: cluster.URL(),
		Provider:         provider,
		ConnectionMode:   ConnectionModeThisNodeOnly,
		Logger:           log.StdoutLogger{},
	})
	require.NoError(t, err)
	require.Equal(t, ConnectionModeThisNodeOnly, client.connectionMode)
}

func TestNewClientWithThisNodeOnlyTooManyAddresses(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	_, err := NewClient(ClientOptions{
		ConnectionString: fmt.Sprintf("%s,secondhostname:8091", cluster.URL()),
		Provider:         provider,
		ConnectionMode:   ConnectionModeThisNodeOnly,
		Logger:           log.StdoutLogger{},
	})
	require.ErrorIs(t, err, ErrThisNodeOnlyExpectsASingleAddress)
}

func TestNewClientWithLoopbackWithTLS(t *testing.T) {
	_, err := NewClient(ClientOptions{
		ConnectionString: "https://localhost:8091",
		Provider:         provider,
		ConnectionMode:   ConnectionModeLoopback,
		Logger:           log.StdoutLogger{},
	})
	require.ErrorIs(t, err, ErrConnectionModeRequiresNonTLS)
}

func TestNewClient(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	// Don't compare the time attribute from the config manager
	client.authProvider.manager.last = nil
	client.authProvider.manager.signal = nil
	client.authProvider.manager.cond = nil

	expected := &AuthProvider{
		resolved: &connstr.ResolvedConnectionString{
			Addresses: []connstr.Address{{
				Host: cluster.Address(),
				Port: cluster.Port(),
			}},
		},
		provider: provider,
		manager: &ClusterConfigManager{
			config: &ClusterConfig{
				Nodes: Nodes{{
					Hostname: cluster.Address(),
					Services: &Services{Management: cluster.Port()},
				}},
			},
			maxAge: DefaultCCMaxAge,
			logger: log.NewWrappedLogger(log.StdoutLogger{}),
		},
	}

	require.Equal(t, expected, client.authProvider)
}

func TestNewClientInvalidConnectionString(t *testing.T) {
	_, err := NewClient(ClientOptions{ConnectionString: "abcd://efgh.ijkl"})
	require.ErrorContains(t, err, "failed to parse connection string")
}

func TestNewClientThisNodeOnly(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{Nodes: TestNodes{{}, {}, {}, {}}})
	defer cluster.Close()

	client, err := NewClient(ClientOptions{
		ConnectionString: cluster.URL(),
		Provider:         provider,
		ConnectionMode:   ConnectionModeThisNodeOnly,
		Logger:           log.StdoutLogger{},
	})
	require.NoError(t, err)

	defer client.Close()
	require.Len(t, client.authProvider.manager.config.Nodes, 1)
}

// This is a smoke test to assert that the cluster config poller doesn't attempt to dereference a <nil> pointer. See
// MB-46754 for more information.
func TestNewClientBeginCCPAfterClusterInfo(t *testing.T) {
	os.Setenv("CB_REST_CC_MAX_AGE", "50ms")
	defer os.Unsetenv("CB_REST_CC_MAX_AGE")

	handlers := make(TestHandlers)

	handlers.Add(http.MethodGet, string(EndpointPoolsDefault), func(writer http.ResponseWriter, request *http.Request) {
		time.Sleep(100 * time.Millisecond)

		testutil.EncodeJSON(t, writer, struct {
			Nodes []node `json:"nodes"`
		}{
			Nodes: createNodeList(TestNodes{{}}),
		})
	})

	cluster := NewTestCluster(t, TestClusterOptions{Handlers: handlers})
	defer cluster.Close()

	client, err := newTestClient(cluster, false)
	require.NoError(t, err)

	defer client.Close()
}

func TestNewClientClusterNotInitialized(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, string(EndpointPools), NewTestHandler(t, http.StatusOK, []byte(`{"uuid":[]}`)))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	_, err := newTestClient(cluster, true)
	require.ErrorIs(t, err, ErrNodeUninitialized)
}

func TestNewClientFailedToBootstrapAgainstHost(t *testing.T) {
	os.Setenv("CB_REST_CLIENT_TIMEOUT_SECS", "1")
	defer os.Unsetenv("CB_REST_CLIENT_TIMEOUT_SECS")

	cluster := NewTestCluster(t, TestClusterOptions{
		Nodes: TestNodes{{}},
	})
	defer cluster.Close()

	client, err := NewClient(ClientOptions{
		ConnectionString: fmt.Sprintf("http://notahost:21345,%s:%d", cluster.Address(), cluster.Port()),
		DisableCCP:       true,
		Provider:         provider,
		Logger:           log.StdoutLogger{},
	})
	require.NoError(t, err)

	// Don't compare the time attribute from the config manager
	client.authProvider.manager.last = nil
	client.authProvider.manager.signal = nil
	client.authProvider.manager.cond = nil

	expected := &AuthProvider{
		resolved: &connstr.ResolvedConnectionString{
			Addresses: []connstr.Address{
				{
					Host: "notahost",
					Port: 21345,
				},
				{
					Host: cluster.Address(),
					Port: cluster.Port(),
				},
			},
		},
		provider: provider,
		manager: &ClusterConfigManager{
			config: &ClusterConfig{
				Nodes: cluster.Nodes(),
			},
			maxAge: DefaultCCMaxAge,
			logger: log.NewWrappedLogger(log.StdoutLogger{}),
		},
	}

	require.Equal(t, expected, client.authProvider)
}

func TestNewClientFailedToBootstrapAgainstAnyHost(t *testing.T) {
	os.Setenv("CB_REST_CLIENT_TIMEOUT_SECS", "1")
	defer os.Unsetenv("CB_REST_CLIENT_TIMEOUT_SECS")

	_, err := NewClient(ClientOptions{
		ConnectionString: "http://notahost:21345,notanotherhost:12355",
		DisableCCP:       true,
		Provider:         provider,
		Logger:           log.StdoutLogger{},
	})

	var bootstrapFailure *BootstrapFailureError

	require.ErrorAs(t, err, &bootstrapFailure)
	require.Nil(t, bootstrapFailure.ErrAuthentication)
}

func TestNewClientFailedToBootstrapAgainstAnyHostUnauthorized(t *testing.T) {
	handlers := make(TestHandlers)

	handlers.Add(
		http.MethodGet,
		string(EndpointNodesServices),
		NewTestHandler(t, http.StatusUnauthorized, make([]byte, 0)),
	)

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	_, err := newTestClient(cluster, true)

	var bootstrapFailure *BootstrapFailureError

	require.ErrorAs(t, err, &bootstrapFailure)
	require.NotNil(t, bootstrapFailure.ErrAuthentication)
}

func TestNewClientFailedToBootstrapAgainstAnyHostForbidden(t *testing.T) {
	handlers := make(TestHandlers)

	handlers.Add(
		http.MethodGet,
		string(EndpointNodesServices),
		NewTestHandler(t, http.StatusForbidden, make([]byte, 0)),
	)

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	_, err := newTestClient(cluster, true)

	var bootstrapFailure *BootstrapFailureError

	require.ErrorAs(t, err, &bootstrapFailure)
	require.NotNil(t, bootstrapFailure.ErrAuthorization)
}

func TestNewClientForcedExternalNetworkMode(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{
		Nodes: TestNodes{{AltAddress: true}},
	})
	defer cluster.Close()

	client, err := NewClient(ClientOptions{
		ConnectionString: cluster.URL() + "?network=external",
		DisableCCP:       true,
		Provider:         provider,
		Logger:           log.StdoutLogger{},
	})
	require.NoError(t, err)

	defer client.Close()

	// Don't compare the time attribute from the config manager
	client.authProvider.manager.last = nil
	client.authProvider.manager.signal = nil
	client.authProvider.manager.cond = nil

	expected := &AuthProvider{
		resolved: &connstr.ResolvedConnectionString{
			Addresses: []connstr.Address{
				{
					Host: cluster.Address(),
					Port: cluster.Port(),
				},
			},
			Params: url.Values{"network": {"external"}},
		},
		useAltAddr: true,
		provider:   provider,
		manager: &ClusterConfigManager{
			config: &ClusterConfig{
				Nodes: cluster.Nodes(),
			},
			maxAge: DefaultCCMaxAge,
			logger: log.NewWrappedLogger(log.StdoutLogger{}),
		},
	}

	require.Equal(t, expected, client.authProvider)
}

func TestNewClientForcedExternalNetworkModeNoExternal(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	_, err := NewClient(ClientOptions{
		ConnectionString: cluster.URL() + "?network=external",
		DisableCCP:       true,
		Provider:         provider,
	})

	// External networking isn't enabled on the cluster, therefore, we should be unable to find a node running the
	// management service.
	var errNotAvailable *ServiceNotAvailableError

	require.ErrorAs(t, err, &errNotAvailable)
}

func TestNewClientTLS(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{
		Nodes:     TestNodes{{SSL: true}},
		TLSConfig: &tls.Config{},
	})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	// Don't compare the time attribute from the config manager
	client.authProvider.manager.last = nil
	client.authProvider.manager.signal = nil
	client.authProvider.manager.cond = nil

	expected := &AuthProvider{
		resolved: &connstr.ResolvedConnectionString{
			Addresses: []connstr.Address{{
				Host: cluster.Address(),
				Port: cluster.Port(),
			}},
			UseSSL: true,
		},
		provider: provider,
		manager: &ClusterConfigManager{
			config: &ClusterConfig{
				Nodes: cluster.Nodes(),
			},
			maxAge: DefaultCCMaxAge,
			logger: log.NewWrappedLogger(log.StdoutLogger{}),
		},
	}

	require.Equal(t, expected, client.authProvider)
}

func TestNewClientTLSReturnX509Errors(t *testing.T) {
	// NOTE: These test certificates are simply those as generated by using all the defaults when using OpenSSL, this
	// results in a 'x509: cannot validate certificate for 127.0.0.1 because it doesn't contain any IP SANs' error.
	rawCert := `
-----BEGIN CERTIFICATE-----
MIIDazCCAlOgAwIBAgIUfD5CjLfwV+NT7MQXucWoWPqBwQAwDQYJKoZIhvcNAQEL
BQAwRTELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoM
GEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDAeFw0yMTA3MjgxMTMwNDFaFw0yMTA4
MjcxMTMwNDFaMEUxCzAJBgNVBAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEw
HwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQC1FVv40rAT9+dVXEbXykrNshCCG+jPmrCs8rkVMv3L
WR93Roh1DWzA/wDDIqxwORoUsWxWMRpfP8o4W2AibZSjEgcyqEqJLHNHVkFned0q
zN9aZ1uXd964JPDX866l9EPB+vetiCpspQTy/ia2y8bseBHTpxaEKbR9t7l1tC5B
/39lhbN1AbAMZ1eQGeXfYxippFUAxsNG8CrrUYZ4E+2o4o2gZAR+O+aNUwV7oKgi
6+T+fzONLQ4K97VwJtsv/u93bqgMdlb4qfYk4X8/ah+qDRU7KWCad/zqz+vbBM0a
Gfar5H+3jzpCrhcWe1M6CHblnwh+kMSEw1rXgZvNPyB3AgMBAAGjUzBRMB0GA1Ud
DgQWBBSwQ8Pz183Tz4FFuqe2gu9Jm608zDAfBgNVHSMEGDAWgBSwQ8Pz183Tz4FF
uqe2gu9Jm608zDAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBV
TJsZ66TNtBLM99zArbnk+mOuZmGQ/71quYRQMwIuAGJo06JlqJepxOSrLiwEQG7O
zbzisl7dxCTh59e552TsKvLL1wIX3YyIqxAbtkVGVD2dNnAU+rBhgacostXWtiWM
7HBJ4IWEQaQ57SdbBlEmr0BaC1gmKuw8ciEx5kMZBH99L7io1L49wXstsTiyKKaL
kDpWI0l39J+UqQzsevNQ6U1isomVKLYOnu+JuOOfj9YGeo/hGei1HJpRJnYSeGfv
buwiyfoL9/vdCfg5Fcf+EzfRJ2KqP4ZqdWpWK2jXNWF/2CGuhURgUgqrrXw3orXr
gSWD/Lms39IxrJbkM1RQ
-----END CERTIFICATE-----
	`

	rawKey := `
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC1FVv40rAT9+dV
XEbXykrNshCCG+jPmrCs8rkVMv3LWR93Roh1DWzA/wDDIqxwORoUsWxWMRpfP8o4
W2AibZSjEgcyqEqJLHNHVkFned0qzN9aZ1uXd964JPDX866l9EPB+vetiCpspQTy
/ia2y8bseBHTpxaEKbR9t7l1tC5B/39lhbN1AbAMZ1eQGeXfYxippFUAxsNG8Crr
UYZ4E+2o4o2gZAR+O+aNUwV7oKgi6+T+fzONLQ4K97VwJtsv/u93bqgMdlb4qfYk
4X8/ah+qDRU7KWCad/zqz+vbBM0aGfar5H+3jzpCrhcWe1M6CHblnwh+kMSEw1rX
gZvNPyB3AgMBAAECggEAV5m1nHGP7JkIuCqjutCJz2hMxCRsQ8I5pfDlyHOagzzi
E/lzGe0cp2C1JbEoakG1dD1Ag5HNiDZ7xTevEfig5yJZiRAQvrtaKTB5A23YqFPj
2QAXNvcuLzbvrAiefHnmLXkTSEeO/m/2LPb2E4gQ+2Q6e4FhSx5Bym+OsHoxxorZ
n7BJBJa6VuoVPYWqlgjJyri5P42q6BOnsYzQbOFrG+BhkkKwcscG6C2evgM9tCBh
OKOqA3w2eCoiBSDBZdjrwvPRFLWFjUe6/SoC5lIVnCeYHlKQ1LJjJ8tZttFn/jyY
36b9edXINIf+y1j40f2hBYT0BdQeaNIkyWCvE+UTwQKBgQDwx7RhIloLUGmvbjNK
geX2+WoVwE4NNEancVFxFysz6mxmP6WPnuHfSvxl3ARcfzNkp40pkMVsW4q0OhgI
kcQpznRuukPm2KkAfj1gHUMrjjTiGwJ9iTo+HVnw7Stlz7XxQ5oZzj9ko2lgXIl8
J2KEo8jdJl7gM6UQ2TY5rsTEpQKBgQDAh6UyHSuPb6pbZMzlbb143eCuzz2mMzse
Rlm6mvelziFsJyc2O7r5w8FY7KjMIQ4t1yHR24n7T6/5h6/PyX2/K0YodPY2/4h7
prrcA46qWkSwZaMdZ6WOA4qCkP9U8qFU0/nNtoMLOCtM0vIhJGzRWgMubDgPGuqh
XYgZ7TiZ6wKBgQDoOENQ11eux8xWJNuVBXksXFqjRchBVeS6w0C/6I+DT5lieAoO
XTcNK34II02VhByl/C7aIsU1f4hj0A+z3BosE/EZhc3NS7KbRiWdmDtbO5jnZRNe
zcX9eENxaNxNIiog2Yk7UD61qiEMjdMPidCrimdEWyhv5X9So5t3wIYH/QKBgATV
IstgOhCun9sNa6sylvfqsdIRd52tWYWIBIaPjznFM8eQMAbHdwj/5eGChzYgekei
R8IJvavmMyeTRVF04EglIOyxCuEUhut7ouMU38bc3y6CTpbVXC11X0upsg8CzBPi
Ajoso5trfILAJL26OVUD6mKG6t1OMqd8PodQEgSjAoGAFGZoOvvhOR0D7Phl1EGZ
ZIM1P0yxNfXnlQ6+evTnc73mTJ6zCe+cSl6HszjeEya6zC53jrRgmKeWOE+bfAdV
bR0zF2wY/njCdRDMvihmIpibhAmw0sxpHUbpoa13ikTc4yXrrtN6uT8Wpt7ZtKRU
cCVg2wGSe3uR/Ce3aC3Tr1g=
-----END PRIVATE KEY-----
	`

	cert, err := tls.X509KeyPair([]byte(rawCert), []byte(rawKey))
	require.NoError(t, err)

	cluster := NewTestCluster(t, TestClusterOptions{
		Nodes:     TestNodes{{SSL: true}},
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
	})
	defer cluster.Close()

	_, err = newTestClient(cluster, true)
	require.Error(t, err)

	var errUnknownX509Error *UnknownX509Error

	require.ErrorAs(t, err, &errUnknownX509Error)
}

func TestClientExecute(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusOK, []byte("body")))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	expected := &Response{
		StatusCode: http.StatusOK,
		Body:       []byte("body"),
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	actual, err := client.Execute(request)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestClientExecuteWithOverrideHost(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusTeapot)
	}))

	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()
	defer server.Close()

	request := &Request{
		Host:               server.URL,
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusTeapot,
		Method:             http.MethodGet,
	}

	expected := &Response{
		StatusCode: http.StatusTeapot,
		Body:       make([]byte, 0),
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	actual, err := client.Execute(request)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestClientExecuteRetryWithCCUpdate(t *testing.T) {
	for _, disableCCP := range []bool{false, true} {
		t.Run(fmt.Sprintf(`{"disable_ccp":"%t"}`, disableCCP), func(t *testing.T) {
			handlers := make(TestHandlers)
			handlers.Add(http.MethodGet, "/test", NewTestHandlerWithHijack(t))

			cluster := NewTestCluster(t, TestClusterOptions{
				Handlers: handlers,
			})
			defer cluster.Close()

			request := &Request{
				ContentType:        ContentTypeURLEncoded,
				Endpoint:           "/test",
				ExpectedStatusCode: http.StatusOK,
				Method:             http.MethodGet,
				Service:            ServiceManagement,
			}

			client, err := newTestClient(cluster, disableCCP)
			require.NoError(t, err)
			defer client.Close()

			_, err = client.Execute(request)

			var retriesExhausted *RetriesExhaustedError

			require.ErrorAs(t, err, &retriesExhausted)
			require.Equal(t, int64(3), client.authProvider.manager.config.Revision)
		})
	}
}

func TestClientExecuteRetryResponseWithCCUpdate(t *testing.T) {
	for _, disableCCP := range []bool{false, true} {
		t.Run(fmt.Sprintf(`{"disable_ccp":"%t"}`, disableCCP), func(t *testing.T) {
			handlers := make(TestHandlers)
			handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusUnauthorized, make([]byte, 0)))

			cluster := NewTestCluster(t, TestClusterOptions{
				Handlers: handlers,
			})
			defer cluster.Close()

			request := &Request{
				ContentType:        ContentTypeURLEncoded,
				Endpoint:           "/test",
				ExpectedStatusCode: http.StatusOK,
				Method:             http.MethodGet,
				Service:            ServiceManagement,
			}

			client, err := newTestClient(cluster, disableCCP)
			require.NoError(t, err)
			defer client.Close()

			_, err = client.Execute(request)

			var retriesExhausted *RetriesExhaustedError

			require.ErrorAs(t, err, &retriesExhausted)
			require.Equal(t, int64(3), client.authProvider.manager.config.Revision)
		})
	}
}

func TestClientExecuteWithSkipRetry(t *testing.T) {
	var (
		attempts int
		handlers = make(TestHandlers)
	)

	handlers.Add(http.MethodGet, "/test", func(writer http.ResponseWriter, request *http.Request) {
		attempts++

		writer.WriteHeader(http.StatusGatewayTimeout)

		_, err := writer.Write([]byte("Hello, World!"))
		require.NoError(t, err)
	})

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:          ContentTypeURLEncoded,
		Endpoint:             "/test",
		ExpectedStatusCode:   http.StatusOK,
		Method:               http.MethodGet,
		NoRetryOnStatusCodes: []int{http.StatusGatewayTimeout},
		Service:              ServiceManagement,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	_, err = client.Execute(request)

	var unexpectedStatus *UnexpectedStatusCodeError

	require.ErrorAs(t, err, &unexpectedStatus)
	require.Equal(t, []byte("Hello, World!"), unexpectedStatus.body)
	require.Equal(t, 1, attempts)
}

func TestClientExecuteWithModeLoopback(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusOK, nil))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:          ContentTypeURLEncoded,
		Endpoint:             "/test",
		ExpectedStatusCode:   http.StatusOK,
		Method:               http.MethodGet,
		NoRetryOnStatusCodes: []int{http.StatusGatewayTimeout},
		Service:              ServiceManagement,
	}

	client, err := NewClient(ClientOptions{
		ConnectionString: cluster.URL(),
		DisableCCP:       true,
		ConnectionMode:   ConnectionModeLoopback,
		Provider:         provider,
		Logger:           log.StdoutLogger{},
	})
	require.NoError(t, err)

	// Any requests should timeout if they attempt to use the hostname from the payload, however, we're using loopback
	// mode so they should be ignored.
	for _, node := range client.authProvider.manager.config.Nodes {
		node.Hostname = "not-a-hostname"
	}

	_, err = client.Execute(request)
	require.NoError(t, err)
}

func TestClientExecuteWithDefaultRetries(t *testing.T) {
	for status := range netutil.TemporaryFailureStatusCodes {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			handlers := make(TestHandlers)

			handlers.Add(
				http.MethodGet,
				"/test",
				NewTestHandlerWithRetries(t, 2, status, http.StatusOK, "", []byte("body")),
			)

			cluster := NewTestCluster(t, TestClusterOptions{
				Handlers: handlers,
			})
			defer cluster.Close()

			request := &Request{
				ContentType:        ContentTypeURLEncoded,
				Endpoint:           "/test",
				ExpectedStatusCode: http.StatusOK,
				Method:             http.MethodGet,
				Service:            ServiceManagement,
			}

			expected := &Response{
				StatusCode: http.StatusOK,
				Body:       []byte("body"),
			}

			client, err := newTestClient(cluster, true)
			require.NoError(t, err)
			defer client.Close()

			actual, err := client.Execute(request)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
}

func TestClientExecuteWithRetries(t *testing.T) {
	handlers := make(TestHandlers)

	handlers.Add(
		http.MethodGet,
		"/test",
		NewTestHandlerWithRetries(t, 2, http.StatusTooEarly, http.StatusOK, "", []byte("body")),
	)

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		RetryOnStatusCodes: []int{http.StatusTooEarly},
		Service:            ServiceManagement,
	}

	expected := &Response{
		StatusCode: http.StatusOK,
		Body:       []byte("body"),
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	actual, err := client.Execute(request)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestClientExecuteWithRetryAfter(t *testing.T) {
	type test struct {
		name   string
		status int
		after  func() string // We use a function to ensure durations aren't calculated in advance
		waited bool
	}

	tests := []*test{
		{
			name:   "IntegerNumberOfSeconds",
			status: http.StatusServiceUnavailable,
			after:  func() string { return "1" },
			waited: true,
		},
		{
			name:   "IntegerNumberOfSecondsNot503",
			status: http.StatusGatewayTimeout,
			after:  func() string { return "1" },
		},
		{
			name:   "Date",
			status: http.StatusServiceUnavailable,
			after:  func() string { return time.Now().UTC().Add(2 * time.Second).Format(time.RFC1123) },
			waited: true,
		},
		{
			name:   "DateNotUTC",
			status: http.StatusServiceUnavailable,
			after:  func() string { return time.Now().Add(2 * time.Second).Format(time.RFC1123) },
			waited: true,
		},
		{
			name:   "DateNot503",
			status: http.StatusGatewayTimeout,
			after:  func() string { return time.Now().UTC().Add(2 * time.Second).Format(time.RFC1123) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handlers := make(TestHandlers)

			handlers.Add(
				http.MethodGet,
				"/test",
				NewTestHandlerWithRetries(t, 1, test.status, http.StatusOK, test.after(), make([]byte, 0)),
			)

			cluster := NewTestCluster(t, TestClusterOptions{
				Handlers: handlers,
			})
			defer cluster.Close()

			request := &Request{
				ContentType:        ContentTypeURLEncoded,
				Endpoint:           "/test",
				ExpectedStatusCode: http.StatusOK,
				Method:             http.MethodGet,
				Service:            ServiceManagement,
			}

			expected := &Response{
				StatusCode: http.StatusOK,
				Body:       make([]byte, 0),
			}

			client, err := newTestClient(cluster, true)
			require.NoError(t, err)
			defer client.Close()

			start := time.Now()

			actual, err := client.Execute(request)
			require.NoError(t, err)
			require.Equal(t, expected, actual)

			require.Equal(t, test.waited, time.Since(start) >= time.Second)
		})
	}
}

func TestClientExecuteStandardError(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusOK, []byte("body")))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           EndpointPools,
		ExpectedStatusCode: http.StatusTeapot, // We will not get this status code, so we should error out
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	expected := &Response{
		StatusCode: http.StatusOK,
		Body:       []byte(`{"isEnterprise":false,"uuid":"","isDeveloperPreview":false}` + "\n"),
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	actual, err := client.Execute(request)
	require.Error(t, err)
	require.Equal(t, expected, actual)

	var unexpectedStatus *UnexpectedStatusCodeError

	require.ErrorAs(t, err, &unexpectedStatus)
}

func TestClientExecuteWithNonIdepotentRequest(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodPost, "/test", NewTestHandler(t, http.StatusTooEarly, make([]byte, 0)))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodPost,
		RetryOnStatusCodes: []int{http.StatusTooEarly},
		Service:            ServiceManagement,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	_, err = client.Execute(request)
	require.Error(t, err)

	var (
		retriesExhausted *RetriesExhaustedError
		unexpectedStatus *UnexpectedStatusCodeError
	)

	require.False(t, errors.As(err, &retriesExhausted)) // testify doesn't appear to have a 'NotErrorAs' function...
	require.ErrorAs(t, err, &unexpectedStatus)
}

func TestClientExecuteWithRetriesExhausted(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusTooEarly, make([]byte, 0)))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		RetryOnStatusCodes: []int{http.StatusTooEarly},
		Service:            ServiceManagement,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	_, err = client.Execute(request)
	require.Error(t, err)

	var retriesExhausted *RetriesExhaustedError

	require.ErrorAs(t, err, &retriesExhausted)
}

func TestClientExecuteSpecificServiceNotAvailable(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusTooEarly, make([]byte, 0)))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceAnalytics,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	_, err = client.Execute(request)
	require.Error(t, err)

	var notAvailable *ServiceNotAvailableError

	require.ErrorAs(t, err, &notAvailable)
}

func TestClientExecuteAuthError(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusUnauthorized, make([]byte, 0)))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	_, err = client.Execute(request)
	require.Error(t, err)

	var unauthorized *AuthenticationError

	require.ErrorAs(t, err, &unauthorized)
}

func TestClientExecuteInternalServerError(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusInternalServerError, []byte("response body")))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	_, err = client.Execute(request)
	require.Error(t, err)

	var internalServerError *InternalServerError

	require.ErrorAs(t, err, &internalServerError)
	require.Equal(t, []byte("response body"), internalServerError.body)
}

func TestClientExecute404Status(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusNotFound, make([]byte, 0)))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	_, err = client.Execute(request)
	require.Error(t, err)

	var endpointNotFound *EndpointNotFoundError

	require.ErrorAs(t, err, &endpointNotFound)
}

func TestClientExecuteUnexpectedEOF(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandlerWithEOF(t))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	_, err = client.Execute(request)
	require.Error(t, err)

	var unexpectedEOB *UnexpectedEndOfBodyError

	require.ErrorAs(t, err, &unexpectedEOB)
}

func TestClientExecuteSocketClosedInFlight(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandlerWithHijack(t))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	_, err = client.Execute(request)
	require.Error(t, err)

	var socketClosedInFlight *SocketClosedInFlightError

	require.ErrorAs(t, err, &socketClosedInFlight)
}

func TestClientExecuteUnknownAuthority(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{
		TLSConfig: &tls.Config{},
	})
	defer cluster.Close()

	_, err := NewClient(ClientOptions{
		ConnectionString: cluster.URL(),
		DisableCCP:       true,
		Provider:         provider,
		Logger:           log.StdoutLogger{},
	})
	require.Error(t, err)

	var unknownAuthority *UnknownAuthorityError

	require.ErrorAs(t, err, &unknownAuthority)
}

func TestClientExecuteStream(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandlerWithStream(t, 5, []byte(`"payload"`)))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	stream, err := client.ExecuteStream(request)
	require.NoError(t, err)
	require.NotNil(t, stream)

	var responses int

	for response := range stream {
		require.NoError(t, response.Error)
		require.Equal(t, []byte(`"payload"`), response.Payload)

		responses++
	}

	require.Equal(t, 5, responses)
}

func TestClientExecuteStreamNoTimeout(t *testing.T) {
	os.Setenv("CB_REST_CLIENT_TIMEOUT_SECS", "50ms")
	defer os.Unsetenv("CB_REST_CLIENT_TIMEOUT_SECS")

	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandlerWithStream(t, 5, []byte(strings.Repeat("a", 4096))))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	stream, err := client.ExecuteStream(request)
	require.NoError(t, err)
	require.NotNil(t, stream)

	time.Sleep(100 * time.Millisecond)

	var responses int

	for response := range stream {
		require.NoError(t, response.Error)

		responses++
	}

	require.Equal(t, 5, responses)
}

func TestClientExecuteStreamAcceptMinusOneTimeout(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandlerWithStream(t, 5, []byte(`"payload"`)))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
		Timeout:            -1,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	stream, err := client.ExecuteStream(request)
	require.NoError(t, err)
	require.NotNil(t, stream)

	var responses int

	for response := range stream {
		require.NoError(t, response.Error)

		responses++
	}

	require.Equal(t, 5, responses)
}

func TestClientExecuteStreamDoNotAcceptTimeout(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
		Timeout:            time.Second,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	_, err = client.ExecuteStream(request)
	require.ErrorIs(t, err, ErrStreamWithTimeout)
}

func TestClientExecuteStreamCloseOnContextCancel(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandlerWithStream(t, 5, []byte(`"payload"`)))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.ExecuteStreamWithContext(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, stream)

	var responses int

	for response := range stream {
		require.NoError(t, response.Error)
		require.Equal(t, []byte(`"payload"`), response.Payload)

		cancel()
		responses++
	}

	// NOTE: We expect to see two responses here, and not one because we use a buffered response channel
	require.Equal(t, 2, responses)
}

func TestClientExecuteStreamWithHijack(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandlerWithStreamHijack(t))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.ExecuteStreamWithContext(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, stream)

	var responses int

	for range stream {
		responses++
	}

	require.Zero(t, responses)
}

func TestClientExecuteStreamWithBinaryPayload(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandlerWithStream(t, 1, []byte(`payload`)))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.ExecuteStreamWithContext(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, stream)

	var responses int

	for response := range stream {
		require.NoError(t, response.Error)
		responses++
	}

	require.Equal(t, 1, responses)
}

func TestClientExecuteStreamWithError(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusUnauthorized, make([]byte, 0)))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	stream, err := client.ExecuteStream(request)
	require.Error(t, err)
	require.Nil(t, stream)

	var unauthorized *AuthenticationError

	require.ErrorAs(t, err, &unauthorized)
}

func TestClientExecuteStreamWithUnexpectedStatusCode(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusTeapot, make([]byte, 0)))

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           "/test",
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	stream, err := client.ExecuteStream(request)
	require.Error(t, err)
	require.Nil(t, stream)

	var unexpectedStatus *UnexpectedStatusCodeError

	require.ErrorAs(t, err, &unexpectedStatus)
}

func TestGetServiceHost(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	host, err := client.GetServiceHost(ServiceManagement)
	require.NoError(t, err)
	require.Equal(t, cluster.URL(), host)
}

func TestGetServiceHostServiceConnectionMode(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := NewClient(ClientOptions{
		ConnectionString: cluster.URL(),
		DisableCCP:       true,
		ConnectionMode:   ConnectionModeLoopback,
		Provider:         provider,
		Logger:           log.StdoutLogger{},
	})
	require.NoError(t, err)

	host, err := client.GetServiceHost(ServiceManagement)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("http://localhost:%d", cluster.Port()), host)
}

func TestGetServiceHostTLS(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{
		Nodes:     TestNodes{{SSL: true}},
		TLSConfig: &tls.Config{},
	})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	host, err := client.GetServiceHost(ServiceManagement)
	require.NoError(t, err)
	require.Equal(t, cluster.URL(), host)
}

func TestGetAllServiceHosts(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{
		Nodes: TestNodes{
			{Services: []Service{ServiceData}},
			{Services: []Service{ServiceManagement}},
			{Services: []Service{ServiceData}},
		},
	})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	hosts, err := client.GetAllServiceHosts(ServiceManagement)
	require.NoError(t, err)
	require.Len(t, hosts, 3)

	hosts, err = client.GetAllServiceHosts(ServiceData)
	require.NoError(t, err)
	require.Len(t, hosts, 2)
}

func TestGetAllServiceHostsTLS(t *testing.T) {
	type test struct {
		name     string
		nodes    TestNodes
		expected int
	}

	tests := []*test{
		{
			name:  "NoTLSHosts",
			nodes: TestNodes{{Services: []Service{ServiceData}}},
		},
		{
			name: "MixedTLSHosts",
			nodes: TestNodes{
				{SSL: true, Services: []Service{ServiceData}},
				{Services: []Service{}},
				{Services: []Service{ServiceData}},
			},
			expected: 1,
		},
		{
			name: "AllTLSHosts",
			nodes: TestNodes{
				{SSL: true, Services: []Service{ServiceData}},
				{SSL: true, Services: []Service{}},
				{SSL: true, Services: []Service{ServiceData}},
			},
			expected: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := NewTestCluster(t, TestClusterOptions{
				Nodes:     test.nodes,
				TLSConfig: &tls.Config{},
			})
			defer cluster.Close()

			client, err := newTestClient(cluster, true)

			if test.expected == 0 {
				require.Error(t, err)

				var notAvailable *ServiceNotAvailableError
				require.ErrorAs(t, err, &notAvailable)

				return
			}

			require.NoError(t, err)
			defer client.Close()

			hosts, err := client.GetAllServiceHosts(ServiceManagement)
			require.NoError(t, err)
			require.Len(t, hosts, test.expected)
		})
	}
}

func TestClientUnmarshalCCHostNameEmpty(t *testing.T) {
	client := &Client{}

	config, err := client.unmarshalCC("localhost", []byte(`{"nodesExt":[{"hostname":""}]}`))
	require.NoError(t, err)
	require.Equal(t, "localhost", config.Nodes[0].Hostname)
}

func TestClientUnmarshalCCHostNameEmptyWithAltAddress(t *testing.T) {
	client := &Client{}

	config, err := client.unmarshalCC("localhost", []byte(
		`{"nodesExt":[{"hostname":"","alternateAddresses":{"external":{"hostname":""}}}]}`,
	))
	require.NoError(t, err)
	require.NotEqual(t, "localhost", config.Nodes[0].AlternateAddresses.External.Hostname)
}

func TestClientUnmarshalCCReconstructIPV6Address(t *testing.T) {
	type test struct {
		name     string
		address  string
		expected string
	}

	tests := []*test{
		{
			name:     "UnwrappedShouldReconstruct",
			address:  "::1",
			expected: "[::1]",
		},
		{
			name:     "WrapedShouldNotReconstruct",
			address:  "[::1]",
			expected: "[::1]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &Client{}

			config, err := client.unmarshalCC("localhost", []byte(
				fmt.Sprintf(`{"nodesExt":[{"hostname":"%s"}]}`, test.address),
			))
			require.NoError(t, err)
			require.Equal(t, test.expected, config.Nodes[0].Hostname)
		})
	}
}

func TestClientUnmarshalCCReconstructIPV6AlternateAddress(t *testing.T) {
	type test struct {
		name     string
		address  string
		expected string
	}

	tests := []*test{
		{
			name:     "UnwrappedShouldReconstruct",
			address:  "::1",
			expected: "[::1]",
		},
		{
			name:     "WrapedShouldNotReconstruct",
			address:  "[::1]",
			expected: "[::1]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &Client{}

			config, err := client.unmarshalCC("localhost", []byte(
				fmt.Sprintf(`{"nodesExt":[{"hostname":"","alternateAddresses":{"external":{"hostname":"%s"}}}]}`,
					test.address),
			))
			require.NoError(t, err)
			require.Equal(t, test.expected, config.Nodes[0].AlternateAddresses.External.Hostname)
		})
	}
}

func TestClientBeginCCP(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	var closed bool

	go func() {
		client.wg.Wait()

		closed = true
	}()

	client.Close()
	time.Sleep(50 * time.Millisecond)

	require.True(t, closed)
	require.Nil(t, client.ctx)
	require.Nil(t, client.cancelFunc)
}

func TestClientPollCC(t *testing.T) {
	os.Setenv("CB_REST_CC_MAX_AGE", "50ms")
	defer os.Unsetenv("CB_REST_CC_MAX_AGE")

	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster, false)
	require.NoError(t, err)

	defer client.Close()

	// The test cluster currently doesn't return a revision, we can exploit this to detect whether the cluster config
	// has been updated or not.
	client.authProvider.manager.config.Revision = math.MaxInt64

	time.Sleep(100 * time.Millisecond)

	require.Equal(t, int64(math.MaxInt64), client.authProvider.manager.config.Revision)
}

func TestClientPollCCOldRevisionIgnore(t *testing.T) {
	os.Setenv("CB_REST_CC_MAX_AGE", "50ms")
	defer os.Unsetenv("CB_REST_CC_MAX_AGE")

	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster, false)
	require.NoError(t, err)

	defer client.Close()

	rev := client.authProvider.manager.config.Revision

	time.Sleep(75 * time.Millisecond)

	require.Equal(t, rev+1, client.authProvider.manager.config.Revision)
}

func TestClientUpdateCC(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	rev := client.authProvider.manager.config.Revision

	require.NoError(t, client.updateCC())
	require.Equal(t, rev+1, client.authProvider.manager.config.Revision)
}

func TestClientUpdateCCExhaustedClusterNodes(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	client.authProvider.manager.config.Nodes = make(Nodes, 0)
	rev := client.authProvider.manager.config.Revision

	require.ErrorIs(t, client.updateCC(), ErrExhaustedClusterNodes)
	require.Equal(t, rev, client.authProvider.manager.config.Revision)
}

func TestClientUpdateCCFromNode(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	rev := client.authProvider.manager.config.Revision

	require.NoError(t, client.updateCCFromNode(client.authProvider.manager.config.Nodes[0]))
	require.Equal(t, rev+1, client.authProvider.manager.config.Revision)
}

func TestClientUpdateCCFromNodeThisNodeOnly(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{Nodes: TestNodes{{}, {}, {}, {}}})
	defer cluster.Close()

	client, err := NewClient(ClientOptions{
		ConnectionString: cluster.URL(),
		ConnectionMode:   ConnectionModeThisNodeOnly,
		DisableCCP:       true,
		Provider:         provider,
		Logger:           log.StdoutLogger{},
	})
	require.NoError(t, err)

	rev := client.authProvider.manager.config.Revision

	require.NoError(t, client.updateCCFromNode(client.authProvider.manager.config.Nodes[0]))
	require.Equal(t, rev+1, client.authProvider.manager.config.Revision)
	require.Len(t, client.authProvider.manager.config.Nodes, 1)
}

func TestClientUpdateCCFromHost(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{Nodes: TestNodes{{}, {}, {}, {}}})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	rev := client.authProvider.manager.config.Revision

	require.NoError(t, client.updateCCFromHost(fmt.Sprintf("http://localhost:%d", cluster.Port())))
	require.Equal(t, rev+1, client.authProvider.manager.config.Revision)
	require.Len(t, client.authProvider.manager.config.Nodes, 4)
}

func TestClientUpdateCCFromHostThisNodeOnly(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{Nodes: TestNodes{{}, {}, {}, {}}})
	defer cluster.Close()

	client, err := NewClient(ClientOptions{
		ConnectionString: cluster.URL(),
		ConnectionMode:   ConnectionModeThisNodeOnly,
		DisableCCP:       true,
		Provider:         provider,
		Logger:           log.StdoutLogger{},
	})
	require.NoError(t, err)

	rev := client.authProvider.manager.config.Revision

	require.NoError(t, client.updateCCFromHost(fmt.Sprintf("http://localhost:%d", cluster.Port())))
	require.Equal(t, rev+1, client.authProvider.manager.config.Revision)
	require.Len(t, client.authProvider.manager.config.Nodes, 1)
}

func TestClientValidHost(t *testing.T) {
	type test struct {
		name     string
		info     *clusterInfo
		body     []byte
		expected bool
	}

	tests := []*test{
		{
			name:     "ValidHost",
			info:     &clusterInfo{UUID: "uuid"},
			body:     []byte(`{"uuid":"uuid"}`),
			expected: true,
		},
		{
			name: "InvalidHostFromAnotherCluster",
			info: &clusterInfo{UUID: "uuid"},
			body: []byte(`{"uuid":"another_uuid"}`),
		},
		{
			name: "InvalidHostUninitialized",
			info: &clusterInfo{UUID: "uuid"},
			body: []byte(`{"uuid":[]}`),
		},
		{
			name:     "ValidHostNotGotClusterInfoYet",
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handlers := make(TestHandlers)
			handlers.Add(http.MethodGet, string(EndpointPools), NewTestHandler(t, http.StatusOK, test.body))

			var uuid string
			if test.info != nil {
				uuid = test.info.UUID
			}

			cluster := NewTestCluster(t, TestClusterOptions{UUID: uuid, Handlers: handlers})
			defer cluster.Close()

			client := &Client{
				client: &http.Client{},
				authProvider: NewAuthProvider(AuthProviderOptions{
					&connstr.ResolvedConnectionString{},
					provider,
					log.StdoutLogger{},
				}),
				clusterInfo: test.info,
				logger:      log.NewWrappedLogger(log.StdoutLogger{}),
			}

			valid, err := client.validHost(fmt.Sprintf("http://localhost:%d", cluster.Port()))
			require.NoError(t, err)
			require.Equal(t, test.expected, valid)
		})
	}
}

func TestClientGetWithRequestError(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandlerWithHijack(t))

	cluster := NewTestCluster(t, TestClusterOptions{Handlers: handlers})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	_, err = client.get(fmt.Sprintf("http://localhost:%d", cluster.Port()), Endpoint("/test"))

	var socketClosedInFlight *SocketClosedInFlightError

	require.ErrorAs(t, err, &socketClosedInFlight)
}

func TestClientGetWithUnexpectedStatusCode(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusTeapot, make([]byte, 0)))

	cluster := NewTestCluster(t, TestClusterOptions{Handlers: handlers})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	_, err = client.get(fmt.Sprintf("http://localhost:%d", cluster.Port()), Endpoint("/test"))

	var unexpectedStatus *UnexpectedStatusCodeError

	require.ErrorAs(t, err, &unexpectedStatus)
}

func TestClientDoRequestWithCustomTimeout(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(400 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	cluster := NewTestCluster(t, TestClusterOptions{
		Handlers: handlers,
	})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	defer client.Close()

	t.Run("custom larger than client", func(t *testing.T) {
		client.client.Timeout = 100 * time.Millisecond

		res, err := client.Execute(
			&Request{
				Method:             http.MethodGet,
				Endpoint:           "/test",
				ExpectedStatusCode: http.StatusOK,
				Service:            ServiceManagement,
				Timeout:            800 * time.Millisecond,
			},
		)

		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode)
	})

	t.Run("custom smaller than client", func(t *testing.T) {
		client.client.Timeout = 800 * time.Millisecond

		res, err := client.Execute(
			&Request{
				Method:             http.MethodGet,
				Endpoint:           "/test",
				ExpectedStatusCode: http.StatusOK,
				Service:            ServiceManagement,
				Timeout:            100 * time.Millisecond,
			},
		)

		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode)
	})
}

func TestClientWaitUntilUpdated(t *testing.T) {
	for _, connectionMode := range SupportedConnectionModes {
		t.Run(fmt.Sprintf(`{"connection_mode":%d}`, connectionMode), func(t *testing.T) {
			cluster := NewTestCluster(t, TestClusterOptions{})
			defer cluster.Close()

			client, err := NewClient(ClientOptions{
				ConnectionString: cluster.URL(),
				Provider:         provider,
				ConnectionMode:   connectionMode,
				DisableCCP:       true,
				Logger:           log.StdoutLogger{},
			})
			require.NoError(t, err)

			rev := client.authProvider.manager.config.Revision

			client.waitUntilUpdated(context.Background())

			if connectionMode == ConnectionModeThisNodeOnly || connectionMode == ConnectionModeLoopback {
				require.Equal(t, rev, client.authProvider.manager.config.Revision)
			} else {
				require.NotEqual(t, rev, client.authProvider.manager.config.Revision)
			}
		})
	}
}
