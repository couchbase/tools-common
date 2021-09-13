package cbrest

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/tools-common/aprov"
	"github.com/couchbase/tools-common/cbvalue"
	"github.com/couchbase/tools-common/connstr"
	"github.com/couchbase/tools-common/netutil"
	"github.com/couchbase/tools-common/testutil"

	"github.com/stretchr/testify/require"
)

const (
	username  = "username"
	password  = "password"
	userAgent = "user-agent"
)

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
		Provider:         &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
		TLSConfig:        &tls.Config{RootCAs: pool},
	})
}

func TestNewClientWithTransportDefaults(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	transport := client.client.Transport.(*http.Transport)

	require.Zero(t, transport.ExpectContinueTimeout)
	require.Equal(t, 10*time.Second, transport.TLSHandshakeTimeout)
	require.Equal(t, 100, transport.MaxIdleConns)
	require.Equal(t, 90*time.Second, transport.IdleConnTimeout)
	require.NotNil(t, transport.DialContext)
	require.NotNil(t, transport.Proxy)
	require.NotNil(t, transport.TLSClientConfig)
	require.True(t, transport.ForceAttemptHTTP2)
}

func TestNewClient(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

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
		provider: &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
		manager: &ClusterConfigManager{
			config: &ClusterConfig{
				Nodes: Nodes{{
					Hostname:           cluster.Address(),
					Services:           &Services{Management: cluster.Port()},
					AlternateAddresses: AlternateAddresses{},
				}},
			},
			maxAge: DefaultCCMaxAge,
		},
	}

	require.Equal(t, expected, client.authProvider)
}

func TestNewClientThisNodeOnly(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{Nodes: TestNodes{{}, {}, {}, {}}})
	defer cluster.Close()

	client, err := NewClient(ClientOptions{
		ConnectionString: cluster.URL(),
		Provider:         &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
		ThisNodeOnly:     true,
	})
	require.NoError(t, err)
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

	_, err := newTestClient(cluster, false)
	require.NoError(t, err)
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
		Provider:         &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
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
		provider: &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
		manager: &ClusterConfigManager{
			config: &ClusterConfig{
				Nodes: cluster.Nodes(),
			},
			maxAge: DefaultCCMaxAge,
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
		Provider:         &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
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

func TestNewClientAltAddress(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{
		Nodes: TestNodes{{AltAddress: true}},
	})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

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
		},
		provider: &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
		manager: &ClusterConfigManager{
			config: &ClusterConfig{
				Nodes: cluster.Nodes(),
			},
			maxAge: DefaultCCMaxAge,
		},
		useAltAddr: true,
	}

	require.Equal(t, expected, client.authProvider)
}

func TestNewClientTLS(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{
		Nodes:     TestNodes{{SSL: true}},
		TLSConfig: &tls.Config{},
	})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

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
		provider: &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
		manager: &ClusterConfigManager{
			config: &ClusterConfig{
				Nodes: cluster.Nodes(),
			},
			maxAge: DefaultCCMaxAge,
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

	_, err = client.Execute(request)

	var unexpectedStatus *UnexpectedStatusCodeError

	require.ErrorAs(t, err, &unexpectedStatus)
	require.Equal(t, []byte("Hello, World!"), unexpectedStatus.body)
	require.Equal(t, 1, attempts)
}

func TestClientExecuteWithDefaultRetries(t *testing.T) {
	for status := range netutil.TemporaryFailureStatusCodes {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			handlers := make(TestHandlers)

			handlers.Add(
				http.MethodGet,
				"/test",
				NewTestHandlerWithRetries(t, 2, status, http.StatusOK, []byte("body")),
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
		NewTestHandlerWithRetries(t, 2, http.StatusTooEarly, http.StatusOK, []byte("body")),
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

	actual, err := client.Execute(request)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
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
		Body:       []byte(`{"isEnterprise":false,"uuid":""}` + "\n"),
	}

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	actual, err := client.Execute(request)
	require.Error(t, err)
	require.Equal(t, expected, actual)

	var unexpectedStatus *UnexpectedStatusCodeError

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
		Provider:         &aprov.Static{Username: username, Password: password, UserAgent: userAgent},
	})
	require.Error(t, err)

	var unknownAuthority *UnknownAuthorityError

	require.ErrorAs(t, err, &unknownAuthority)
}

func TestGetServiceHost(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	host, err := client.GetServiceHost(ServiceManagement)
	require.NoError(t, err)
	require.Equal(t, cluster.URL(), host)
}

func TestGetServiceHostTLS(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{
		Nodes:     TestNodes{{SSL: true}},
		TLSConfig: &tls.Config{},
	})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

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

func TestGetClusterVersion(t *testing.T) {
	type test struct {
		name     string
		nodes    TestNodes
		expected cbvalue.ClusterVersion
	}

	tests := []*test{
		{
			name:     "SingleNodeCC",
			nodes:    TestNodes{{Version: cbvalue.Version7_0_0}},
			expected: cbvalue.ClusterVersion{MinVersion: cbvalue.Version7_0_0},
		},
		{
			name:     "MultiNodeCC",
			nodes:    TestNodes{{Version: cbvalue.Version7_0_0}, {Version: cbvalue.Version7_0_0}},
			expected: cbvalue.ClusterVersion{MinVersion: cbvalue.Version7_0_0},
		},
		{
			name: "MixedMultiNodeLowestFirst",
			nodes: TestNodes{
				{Version: cbvalue.Version6_6_0}, {Version: cbvalue.Version7_0_0}, {Version: cbvalue.Version7_0_0},
			},
			expected: cbvalue.ClusterVersion{MinVersion: cbvalue.Version6_6_0, Mixed: true},
		},
		{
			name: "MixedMultiNodeLowestLast",
			nodes: TestNodes{
				{Version: cbvalue.Version7_0_0}, {Version: cbvalue.Version7_0_0}, {Version: cbvalue.Version6_6_0},
			},
			expected: cbvalue.ClusterVersion{MinVersion: cbvalue.Version6_6_0, Mixed: true},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := NewTestCluster(t, TestClusterOptions{
				Nodes: test.nodes,
			})
			defer cluster.Close()

			client, err := newTestClient(cluster, true)
			require.NoError(t, err)

			version, err := client.GetClusterVersion()
			require.NoError(t, err)
			require.Equal(t, test.expected, version)
		})
	}
}

func TestGetClusterMetaData(t *testing.T) {
	type test struct {
		name     string
		input    bool
		expected bool
	}

	tests := []*test{
		{
			name:     "EE",
			input:    true,
			expected: true,
		},
		{
			name: "CE",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := NewTestCluster(t, TestClusterOptions{
				Enterprise: test.input,
			})
			defer cluster.Close()

			client, err := newTestClient(cluster, true)
			require.NoError(t, err)

			enterprise, _, err := client.GetClusterMetaData()
			require.NoError(t, err)
			require.Equal(t, test.expected, enterprise)
		})
	}
}

func TestGetMaxVBuckets(t *testing.T) {
	type test struct {
		name    string
		buckets TestBuckets
		max     uint16
		uniform bool
	}

	tests := []*test{
		{
			name:    "NoBuckets",
			uniform: true,
		},
		{
			name: "AllTheSame",
			buckets: TestBuckets{
				"bucket1": {NumVBuckets: 64},
				"bucket2": {NumVBuckets: 64},
			},
			max:     64,
			uniform: true,
		},
		{
			name: "FirstWithMore",
			buckets: TestBuckets{
				"bucket1": {NumVBuckets: 1024},
				"bucket2": {NumVBuckets: 64},
				"bucket3": {NumVBuckets: 64},
			},
			max: 1024,
		},
		{
			name: "MiddleWithMore",
			buckets: TestBuckets{
				"bucket1": {NumVBuckets: 64},
				"bucket2": {NumVBuckets: 1024},
				"bucket3": {NumVBuckets: 64},
			},
			max: 1024,
		},
		{
			name: "LastWithMore",
			buckets: TestBuckets{
				"bucket1": {NumVBuckets: 64},
				"bucket2": {NumVBuckets: 64},
				"bucket3": {NumVBuckets: 1024},
			},
			max: 1024,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := NewTestCluster(t, TestClusterOptions{Buckets: test.buckets})
			defer cluster.Close()

			client, err := newTestClient(cluster, true)
			require.NoError(t, err)

			maxVBuckets, uniformVBuckets, err := client.GetMaxVBuckets()
			require.NoError(t, err)
			require.Equal(t, test.max, maxVBuckets)
			require.Equal(t, test.uniform, uniformVBuckets)
		})
	}
}

func TestClientBeginCCP(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

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

	rev := client.authProvider.manager.config.Revision

	require.NoError(t, client.updateCC())
	require.Equal(t, rev+1, client.authProvider.manager.config.Revision)
}

func TestClientUpdateCCExhaustedClusterNodes(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

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

	rev := client.authProvider.manager.config.Revision

	require.NoError(t, client.updateCCFromNode(client.authProvider.manager.config.Nodes[0]))
	require.Equal(t, rev+1, client.authProvider.manager.config.Revision)
}

func TestClientUpdateCCFromHost(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{Nodes: TestNodes{{}, {}, {}, {}}})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	rev := client.authProvider.manager.config.Revision

	require.NoError(t, client.updateCCFromHost(fmt.Sprintf("http://localhost:%d", cluster.Port()), false))
	require.Equal(t, rev+1, client.authProvider.manager.config.Revision)
	require.Len(t, client.authProvider.manager.config.Nodes, 4)
}

func TestClientUpdateCCFromHostThisNodeOnly(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{Nodes: TestNodes{{}, {}, {}, {}}})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	rev := client.authProvider.manager.config.Revision

	require.NoError(t, client.updateCCFromHost(fmt.Sprintf("http://localhost:%d", cluster.Port()), true))
	require.Equal(t, rev+1, client.authProvider.manager.config.Revision)
	require.Len(t, client.authProvider.manager.config.Nodes, 1)
}

func TestClientValidHost(t *testing.T) {
	type test struct {
		name     string
		info     *cbvalue.ClusterInfo
		body     []byte
		expected bool
	}

	tests := []*test{
		{
			name:     "ValidHost",
			info:     &cbvalue.ClusterInfo{UUID: "uuid"},
			body:     []byte(`{"uuid":"uuid"}`),
			expected: true,
		},
		{
			name: "InvalidHostFromAnotherCluster",
			info: &cbvalue.ClusterInfo{UUID: "uuid"},
			body: []byte(`{"uuid":"another_uuid"}`),
		},
		{
			name: "InvalidHostUninitialized",
			info: &cbvalue.ClusterInfo{UUID: "uuid"},
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
				authProvider: NewAuthProvider(
					&connstr.ResolvedConnectionString{},
					&aprov.Static{Username: "username", Password: "password"},
				),
				clusterInfo: test.info,
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
