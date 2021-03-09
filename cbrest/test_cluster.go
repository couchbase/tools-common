package cbrest

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/tools-common/cbvalue"
	"github.com/couchbase/tools-common/netutil"

	"github.com/stretchr/testify/require"
)

// TestClusterOptions encapsulates the options which can be passed when creating a new test cluster. These options
// configure the behavior/setup of the cluster.
type TestClusterOptions struct {
	// Used for the /pools endpoint
	Enterprise bool
	UUID       string

	// Used for both the /pools/default and the /pools/default/nodeServices endpoint
	Nodes TestNodes

	// Additional handler functions which are run to handle a REST request dispatched to the cluster
	Handlers TestHandlers

	// A non-nil TLS config indicates that the cluster should use TLS
	TLSConfig *tls.Config
}

// TestCluster is a mock Couchbase cluster used for unit testing functionaility which relies on the REST client.
type TestCluster struct {
	t       *testing.T
	server  *httptest.Server
	options TestClusterOptions
}

// NewTestCluster creates a new test cluster using the provided options.
//
// NOTE: By default, the /pools, /pools/default and /pools/default/nodeServices endpoints are implemented and their
// return values can be manipulated via the cluster options. These endpoints can be overridden if required, however,
// note that they will still be required to return valid data which can be used to bootstrap the client.
func NewTestCluster(t *testing.T, options TestClusterOptions) *TestCluster {
	if len(options.Nodes) == 0 {
		options.Nodes = TestNodes{{}}
	}

	if options.Handlers == nil {
		options.Handlers = make(TestHandlers)
	}

	cluster := &TestCluster{
		t:       t,
		options: options,
	}

	// def will set the provided endpoint in the handlers if there isn't already a definition.
	def := func(endpoints TestHandlers, method string, endpoint Endpoint,
		handler func(writer http.ResponseWriter, request *http.Request)) {
		_, ok := endpoints[fmt.Sprintf("%s:%s", method, endpoint)]
		if ok {
			return
		}

		endpoints.Add(method, string(endpoint), handler)
	}

	def(options.Handlers, http.MethodGet, EndpointPools, cluster.Pools)
	def(options.Handlers, http.MethodGet, EndpointPoolsDefault, cluster.PoolsDefault)
	def(options.Handlers, http.MethodGet, EndpointNodesServices, cluster.NodeServices)

	r := rand.New(rand.NewSource(time.Now().Unix()))

	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", r.Intn(65535-49152)+49152))
	require.NoError(t, err)

	cluster.server = &httptest.Server{
		Config:   &http.Server{Handler: http.HandlerFunc(cluster.Handler)},
		Listener: listener,
		TLS:      options.TLSConfig,
	}

	if options.TLSConfig != nil {
		cluster.server.StartTLS()
	} else {
		cluster.server.Start()
	}

	return cluster
}

// URL returns the fully qualified URL which can be used to connect to the cluster.
func (t *TestCluster) URL() string {
	return t.server.URL
}

// Hostname returns the cluster hostname, for the time being this will always be "localhost".
func (t *TestCluster) Hostname() string {
	return "localhost"
}

// Address returns the address of the cluster, for the time being should always be "127.0.0.1".
func (t *TestCluster) Address() string {
	trimmed := netutil.TrimSchema(t.server.URL)
	return trimmed[:strings.Index(trimmed, ":")]
}

// Port returns the port which requests should be sent to; this will be the same port for all services.
//
// NOTE: This port is randomly selected at runtime and will therefore vary.
func (t *TestCluster) Port() uint16 {
	url, err := url.Parse(t.server.URL)
	require.NoError(t.t, err)

	parsed, err := strconv.Atoi(url.Port())
	require.NoError(t.t, err)

	return uint16(parsed)
}

// Certificate returns the certificate which can be used to authenticate the cluster.
//
// NOTE: This will be <nil> if the cluster is not running with TLS enabled.
func (t *TestCluster) Certificate() *x509.Certificate {
	return t.server.Certificate()
}

// Handler is the base handler function for requests, additional endpoint handlers may be added using the 'Handlers'
// attribute of the cluster options.
//
// NOTE: The current test will fatally terminate if no valid handler is found.
func (t *TestCluster) Handler(writer http.ResponseWriter, request *http.Request) {
	require.Truef(
		t.t,
		t.options.Handlers.Handle(writer, request),
		"Endpoint '%s' does not have a handler",
		request.URL.Path,
	)
}

// Pools implements the /pools endpoint, the return values can be modified using the cluster options.
func (t *TestCluster) Pools(writer http.ResponseWriter, request *http.Request) {
	rJSON, err := json.Marshal(struct {
		Enterprise bool   `json:"isEnterprise"`
		UUID       string `json:"uuid"`
	}{
		Enterprise: t.options.Enterprise,
		UUID:       t.options.UUID,
	})
	require.NoError(t.t, err)

	_, err = writer.Write(rJSON)
	require.NoError(t.t, err)
}

// PoolsDefault implements the /pools/default endpoint, values can be modified by modifying the nodes in the cluster
// using the cluster options.
func (t *TestCluster) PoolsDefault(writer http.ResponseWriter, request *http.Request) {
	type node struct {
		Version cbvalue.Version `json:"version"`
		Status  string          `json:"status"`
	}

	nodes := make([]node, 0, len(t.options.Nodes))
	for _, n := range t.options.Nodes {
		nodes = append(nodes, node{
			Version: n.Version,
			Status:  n.Status,
		})
	}

	rJSON, err := json.Marshal(struct {
		Nodes []node `json:"nodes"`
	}{
		Nodes: nodes,
	})
	require.NoError(t.t, err)

	_, err = writer.Write(rJSON)
	require.NoError(t.t, err)
}

// NodeServices implements the /pools/default/nodeServices endpoint, values can be modified by modifying the nodes in
// the cluster using the cluster options.
func (t *TestCluster) NodeServices(writer http.ResponseWriter, request *http.Request) {
	rJSON, err := json.Marshal(struct {
		Nodes Nodes `json:"nodesExt"`
	}{
		Nodes: t.Nodes(),
	})
	require.NoError(t.t, err)

	_, err = writer.Write(rJSON)
	require.NoError(t.t, err)
}

// Nodes returns the list of nodes in the cluster, generated using the test nodes provided in the cluster options.
func (t *TestCluster) Nodes() Nodes {
	nodes := make([]*Node, 0, len(t.options.Nodes))
	for _, node := range t.options.Nodes {
		nodes = append(nodes, t.createNode(node))
	}

	return nodes
}

// createNode creates a new node using the test node options to determine the specific setup.
func (t *TestCluster) createNode(n *TestNode) *Node {
	port := t.Port()

	node := &Node{
		Hostname: t.Address(),
		Services: &Services{
			Management: port,
		},
	}

	if n.SSL {
		node.Services.ManagementSSL = port
	}

	for _, service := range n.Services {
		addService(node.Services, service, n.SSL, port)
	}

	if n.AltAddress {
		node.AlternateAddresses = &AlternateAddresses{
			Hostname: t.Address(),
			Services: node.Services,
		}

		node.Hostname = t.Hostname()
	}

	if n.OverrideHostname != nil && !n.AltAddress {
		node.Hostname = string(n.OverrideHostname)
	}

	if n.OverrideHostname != nil && n.AltAddress {
		node.AlternateAddresses.Hostname = string(n.OverrideHostname)
	}

	return node
}

// Close stops the server releasing any held resources.
func (t *TestCluster) Close() {
	t.server.Close()
}

// addService is a utility function to add a service to the services structure using the provided settings.
func addService(services *Services, service Service, ssl bool, port uint16) {
	switch service {
	case ServiceAnalytics:
		if ssl {
			services.CBASSSL = port
		}

		services.CBAS = port
	case ServiceData, ServiceViews:
		if ssl {
			services.KVSSL = port
			services.CAPISSL = port
		}

		services.KV = port
		services.CAPI = port
	case ServiceEventing:
		if ssl {
			services.EventingSSL = port
		}

		services.Eventing = port
	case ServiceGSI:
		if ssl {
			services.SecondaryIndexSSL = port
		}

		services.SecondaryIndex = port
	case ServiceQuery:
		if ssl {
			services.N1QLSSL = port
		}

		services.N1QL = port
	case ServiceSearch:
		if ssl {
			services.FullTextSSL = port
		}

		services.FullText = port
	case ServiceBackup:
		if ssl {
			services.BackupSSL = port
		}

		services.Backup = port
	}
}