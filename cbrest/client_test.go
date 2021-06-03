package cbrest

import (
	"context"
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

	require.Equal(t, 1*time.Second, transport.ExpectContinueTimeout)
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

func TestClientExecuteContextCancelledDoNotContinueRetries(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusTooEarly, []byte("body")))

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

	client.requestRetries = math.MaxInt64
	client.requestTimeout = 50 * time.Millisecond

	_, err = client.Execute(request)
	require.ErrorIs(t, err, context.DeadlineExceeded)
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
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster, true)
	require.NoError(t, err)

	rev := client.authProvider.manager.config.Revision

	require.NoError(t, client.updateCCFromHost(fmt.Sprintf("http://localhost:%d", cluster.Port())))
	require.Equal(t, rev+1, client.authProvider.manager.config.Revision)
}

func TestClientValidHost(t *testing.T) {
	type test struct {
		name     string
		uuid     string
		body     []byte
		expected bool
	}

	tests := []*test{
		{
			name:     "ValidHost",
			uuid:     "uuid",
			body:     []byte(`{"uuid":"uuid"}`),
			expected: true,
		},
		{
			name: "InvalidHostFromAnotherCluster",
			uuid: "uuid",
			body: []byte(`{"uuid":"another_uuid"}`),
		},
		{
			name: "InvalidHostUninitialized",
			uuid: "uuid",
			body: []byte(`{"uuid":[]}`),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handlers := make(TestHandlers)
			handlers.Add(http.MethodGet, string(EndpointPools), NewTestHandler(t, http.StatusOK, test.body))

			cluster := NewTestCluster(t, TestClusterOptions{UUID: test.uuid, Handlers: handlers})
			defer cluster.Close()

			client := &Client{
				client: &http.Client{},
				authProvider: NewAuthProvider(
					&connstr.ResolvedConnectionString{},
					&aprov.Static{Username: "username", Password: "password"},
				),
				clusterInfo: &cbvalue.ClusterInfo{UUID: test.uuid},
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
