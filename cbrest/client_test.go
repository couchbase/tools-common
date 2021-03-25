package cbrest

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"

	"github.com/couchbase/tools-common/auth"
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
func newTestClient(cluster *TestCluster) (*Client, error) {
	pool := x509.NewCertPool()

	if cluster.Certificate() != nil {
		pool.AddCert(cluster.Certificate())
	}

	return NewClient(ClientOptions{
		ConnectionString: cluster.URL(),
		Username:         username,
		Password:         password,
		UserAgent:        userAgent,
		TLSConfig:        &tls.Config{RootCAs: pool},
	})
}

func TestNewClient(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster)
	require.NoError(t, err)

	expected := &AuthProvider{
		resolved: &connstr.ResolvedConnectionString{
			Addresses: []connstr.Address{{
				Host: cluster.Address(),
				Port: cluster.Port(),
			}},
		},
		increment: true,
		userAgent: userAgent,
		username:  username,
		password:  password,
		nodes: Nodes{{
			Hostname: cluster.Address(),
			Services: &Services{Management: cluster.Port()},
		}},
		mappings: auth.GetHostMappings(),
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

	_, err := newTestClient(cluster)
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
		Username:         username,
		Password:         password,
		UserAgent:        userAgent,
	})
	require.NoError(t, err)

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
		index:     1,
		increment: true,
		userAgent: userAgent,
		username:  username,
		password:  password,
		nodes:     cluster.Nodes(),
		mappings:  auth.GetHostMappings(),
	}

	require.Equal(t, expected, client.authProvider)
}

func TestNewClientFailedToBootstrapAgainstAnyHost(t *testing.T) {
	os.Setenv("CB_REST_CLIENT_TIMEOUT_SECS", "1")
	defer os.Unsetenv("CB_REST_CLIENT_TIMEOUT_SECS")

	_, err := NewClient(ClientOptions{
		ConnectionString: "http://notahost:21345,notanotherhost:12355",
		Username:         username,
		Password:         password,
		UserAgent:        userAgent,
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

	_, err := newTestClient(cluster)

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

	_, err := newTestClient(cluster)

	var bootstrapFailure *BootstrapFailureError

	require.ErrorAs(t, err, &bootstrapFailure)
	require.NotNil(t, bootstrapFailure.ErrAuthorization)
}

func TestNewClientAltAddress(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{
		Nodes: TestNodes{{AltAddress: true}},
	})
	defer cluster.Close()

	client, err := newTestClient(cluster)
	require.NoError(t, err)

	expected := &AuthProvider{
		resolved: &connstr.ResolvedConnectionString{
			Addresses: []connstr.Address{
				{
					Host: cluster.Address(),
					Port: cluster.Port(),
				},
			},
		},
		increment:  true,
		userAgent:  userAgent,
		username:   username,
		password:   password,
		nodes:      cluster.Nodes(),
		mappings:   auth.GetHostMappings(),
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

	client, err := newTestClient(cluster)
	require.NoError(t, err)

	expected := &AuthProvider{
		resolved: &connstr.ResolvedConnectionString{
			Addresses: []connstr.Address{{
				Host: cluster.Address(),
				Port: cluster.Port(),
			}},
			UseSSL: true,
		},
		increment: true,
		userAgent: userAgent,
		username:  username,
		password:  password,
		nodes:     cluster.Nodes(),
		mappings:  auth.GetHostMappings(),
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

	client, err := newTestClient(cluster)
	require.NoError(t, err)

	actual, err := client.Execute(request)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestClientExecuteWithDefaultRetries(t *testing.T) {
	for _, status := range netutil.TemproraryFailureStatusCodes {
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

			client, err := newTestClient(cluster)
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

	client, err := newTestClient(cluster)
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

	client, err := newTestClient(cluster)
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

	client, err := newTestClient(cluster)
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

	client, err := newTestClient(cluster)
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

	client, err := newTestClient(cluster)
	require.NoError(t, err)

	_, err = client.Execute(request)
	require.Error(t, err)

	var unauthorized *AuthenticationError

	require.ErrorAs(t, err, &unauthorized)
}

func TestClientExecuteInternalServerError(t *testing.T) {
	handlers := make(TestHandlers)
	handlers.Add(http.MethodGet, "/test", NewTestHandler(t, http.StatusInternalServerError, make([]byte, 0)))

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

	client, err := newTestClient(cluster)
	require.NoError(t, err)

	_, err = client.Execute(request)
	require.Error(t, err)

	var internalServerError *InternalServerError

	require.ErrorAs(t, err, &internalServerError)
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

	client, err := newTestClient(cluster)
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

	client, err := newTestClient(cluster)
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

	client, err := newTestClient(cluster)
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
		Username:         username,
		Password:         password,
		UserAgent:        userAgent,
	})
	require.Error(t, err)

	var unknownAuthority *UnknownAuthorityError

	require.ErrorAs(t, err, &unknownAuthority)
}

func TestGetNodes(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster)
	require.NoError(t, err)

	nodes, err := client.GetNodes()
	require.NoError(t, err)
	require.Equal(t, cluster.Nodes(), nodes)
}

func TestGetServiceHost(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{})
	defer cluster.Close()

	client, err := newTestClient(cluster)
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

	client, err := newTestClient(cluster)
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

	client, err := newTestClient(cluster)
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

			client, err := newTestClient(cluster)

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

func TestGetNodesHostNameEmpty(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{
		Nodes: TestNodes{
			{}, // Node to bootstrap against
			{OverrideHostname: []byte("")},
		},
	})
	defer cluster.Close()

	client, err := newTestClient(cluster)
	require.NoError(t, err)

	nodes, err := client.GetNodes()
	require.NoError(t, err)

	require.Equal(
		t,
		Nodes{
			{Hostname: cluster.Address(), Services: &Services{Management: cluster.Port()}},
			{Hostname: cluster.Address(), Services: &Services{Management: cluster.Port()}},
		},
		nodes,
	)
}

func TestGetNodesHostNameEmptyWithAltAddress(t *testing.T) {
	cluster := NewTestCluster(t, TestClusterOptions{
		Nodes: TestNodes{
			{AltAddress: true}, // Node to bootstrap against
			{OverrideHostname: []byte(""), AltAddress: true},
		},
	})
	defer cluster.Close()

	client, err := newTestClient(cluster)
	require.NoError(t, err)

	nodes, err := client.GetNodes()
	require.NoError(t, err)

	require.Equal(
		t,
		Nodes{
			{
				Hostname: cluster.Hostname(),
				Services: &Services{Management: cluster.Port()},
				AlternateAddresses: AlternateAddresses{
					External: &External{Hostname: cluster.Address(), Services: &Services{Management: cluster.Port()}},
				},
			},
			{
				// NOTE: We are validating that the 'Hostname' field is not set to the alternate hostname which we used
				// to bootstrap against.
				Services: &Services{Management: cluster.Port()},
				AlternateAddresses: AlternateAddresses{
					External: &External{Hostname: cluster.Address(), Services: &Services{Management: cluster.Port()}},
				},
			},
		},
		nodes,
	)
}

func TestGetNodesReconstructIPV6Address(t *testing.T) {
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
			cluster := NewTestCluster(t, TestClusterOptions{
				Nodes: TestNodes{
					{}, // Node to bootstrap against
					{
						OverrideHostname: []byte(test.address),
					},
				},
			})
			defer cluster.Close()

			client, err := newTestClient(cluster)
			require.NoError(t, err)

			nodes, err := client.GetNodes()
			require.NoError(t, err)

			expected := Nodes{
				{Hostname: cluster.Address(), Services: &Services{Management: cluster.Port()}},
				{Hostname: test.expected, Services: &Services{Management: cluster.Port()}},
			}

			require.Equal(t, expected, nodes)
		})
	}
}

func TestGetNodesReconstructIPV6AlternateAddress(t *testing.T) {
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
			cluster := NewTestCluster(t, TestClusterOptions{
				Nodes: TestNodes{
					{}, // Node to bootstrap against
					{
						AltAddress:          true,
						OverrideAltHostname: []byte(test.address),
					},
				},
			})
			defer cluster.Close()

			client, err := newTestClient(cluster)
			require.NoError(t, err)

			nodes, err := client.GetNodes()
			require.NoError(t, err)

			expected := Nodes{
				{Hostname: cluster.Address(), Services: &Services{Management: cluster.Port()}},
				{
					Hostname: cluster.Hostname(),
					Services: &Services{
						Management: cluster.Port(),
					},
					AlternateAddresses: AlternateAddresses{
						External: &External{
							Hostname: test.expected,
							Services: &Services{Management: cluster.Port()},
						},
					},
				},
			}

			require.Equal(t, expected, nodes)
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

			client, err := newTestClient(cluster)
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

			client, err := newTestClient(cluster)
			require.NoError(t, err)

			enterprise, _, err := client.GetClusterMetaData()
			require.NoError(t, err)
			require.Equal(t, test.expected, enterprise)
		})
	}
}
