package cbrest

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/tools-common/httptools"
	"github.com/couchbase/tools-common/netutil"
	"github.com/couchbase/tools-common/testutil"

	"github.com/stretchr/testify/require"
)

// TestClusterOptions encapsulates the options which can be passed when creating a new test cluster. These options
// configure the behavior/setup of the cluster.
type TestClusterOptions struct {
	// Used for the /pools endpoint
	Enterprise       bool
	UUID             string
	DeveloperPreview bool

	// Used for both the /pools/default and the /pools/default/nodeServices endpoint
	Nodes TestNodes

	// Used for the /pools/default/buckets endpoint
	Buckets TestBuckets

	// Additional handler functions which are run to handle a REST request dispatched to the cluster
	Handlers TestHandlers

	// A non-nil TLS config indicates that the cluster should use TLS
	TLSConfig *tls.Config
}

// TestCluster is a mock Couchbase cluster used for unit testing functionaility which relies on the REST client.
type TestCluster struct {
	t        *testing.T
	revision int64
	server   *httptest.Server
	options  TestClusterOptions
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
	def := func(method string, endpoint httptools.Endpoint,
		handler func(writer http.ResponseWriter, request *http.Request),
	) {
		_, ok := options.Handlers[fmt.Sprintf("%s:%s", method, endpoint)]
		if ok {
			return
		}

		options.Handlers.Add(method, string(endpoint), handler)
	}

	def(http.MethodGet, EndpointPools, cluster.Pools)
	def(http.MethodGet, EndpointPoolsDefault, cluster.PoolsDefault)
	def(http.MethodGet, EndpointNodesServices, cluster.NodeServices)
	def(http.MethodGet, EndpointBuckets, cluster.Buckets)

	for name := range options.Buckets {
		def(http.MethodGet, EndpointBucket.Format(name), cluster.Bucket(name))
		def(http.MethodGet, EndpointBucketManifest.Format(name), cluster.BucketManifest(name))
	}

	if options.TLSConfig != nil {
		cluster.server = httptest.NewUnstartedServer(http.HandlerFunc(cluster.Handler))
		cluster.server.TLS = options.TLSConfig
		cluster.server.StartTLS()
	} else {
		cluster.server = httptest.NewServer(http.HandlerFunc(cluster.Handler))
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
	ok := t.options.Handlers.Handle(writer, request)
	if ok {
		return
	}

	// This is a status endpoint which contains a variable portion, for the time being we'll always respond indicating
	// that the test cluster has the provided manifest id. Note that this endpoint can still be overridden via a test
	// handler if required; this is just a default fallback.
	if regexp.MustCompile(`^\/pools/default\/buckets/\S+/scopes/@ensureManifest/\d+$`).MatchString(request.URL.Path) {
		writer.WriteHeader(http.StatusOK)
		testutil.Write(t.t, writer, make([]byte, 0))

		return
	}

	t.t.Fatalf("Endpoint '%s' does not have a handler", request.URL.Path)
}

// Pools implements the /pools endpoint, the return values can be modified using the cluster options.
func (t *TestCluster) Pools(writer http.ResponseWriter, request *http.Request) {
	testutil.EncodeJSON(t.t, writer, struct {
		Enterprise       bool   `json:"isEnterprise"`
		UUID             string `json:"uuid"`
		DeveloperPreview bool   `json:"isDeveloperPreview"`
	}{
		Enterprise:       t.options.Enterprise,
		UUID:             t.options.UUID,
		DeveloperPreview: t.options.DeveloperPreview,
	})
}

// PoolsDefault implements the /pools/default endpoint, values can be modified by modifying the nodes in the cluster
// using the cluster options.
func (t *TestCluster) PoolsDefault(writer http.ResponseWriter, request *http.Request) {
	testutil.EncodeJSON(t.t, writer, struct {
		Nodes []node `json:"nodes"`
	}{
		Nodes: createNodeList(t.options.Nodes),
	})
}

// Buckets implements the /pools/default/buckets endpoint, values can be modified by modifying the buckets in the
// cluster using the cluster options.
func (t *TestCluster) Buckets(writer http.ResponseWriter, request *http.Request) {
	buckets := make([]bucket, 0, len(t.options.Buckets))
	for n, b := range t.options.Buckets {
		buckets = append(buckets, bucket{
			Name:             n,
			UUID:             b.UUID,
			VBucketServerMap: vbsm{VBucketMap: make([][2]int, b.NumVBuckets)},
			Nodes:            createNodeList(t.options.Nodes),
		})
	}

	testutil.EncodeJSON(t.t, writer, buckets)
}

// Bucket implements the /pools/default/buckets/<bucket> endpoint, values can be modified by modifying the buckets in
// the cluster using the cluster options.
func (t *TestCluster) Bucket(name string) func(writer http.ResponseWriter, request *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		b, ok := t.options.Buckets[name]
		require.True(t.t, ok)

		testutil.EncodeJSON(t.t, writer, bucket{
			Name:             name,
			UUID:             b.UUID,
			VBucketServerMap: vbsm{VBucketMap: make([][2]int, b.NumVBuckets)},
			Nodes:            createNodeList(t.options.Nodes),
		})
	}
}

// BucketManifest implements the /pools/default/buckets/<bucket>/scopes endpoint. The returned manifest may be set using
// the cluster options.
func (t *TestCluster) BucketManifest(name string) func(writer http.ResponseWriter, request *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		b, ok := t.options.Buckets[name]
		require.True(t.t, ok)

		require.NotNil(t.t, writer, b.Manifest)
		testutil.EncodeJSON(t.t, writer, b.Manifest)
	}
}

// NodeServices implements the /pools/default/nodeServices endpoint, values can be modified by modifying the nodes in
// the cluster using the cluster options.
func (t *TestCluster) NodeServices(writer http.ResponseWriter, request *http.Request) {
	defer func() { t.revision++ }()

	testutil.EncodeJSON(t.t, writer, struct {
		Revision int64 `json:"rev"`
		Nodes    Nodes `json:"nodesExt"`
	}{
		Revision: t.revision,
		Nodes:    t.Nodes(),
	})
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
		node.AlternateAddresses.External = &External{
			Hostname: t.Address(),
			Services: node.Services,
		}

		node.Hostname = t.Hostname()
	}

	return node
}

// Close stops the server releasing any held resources.
func (t *TestCluster) Close() {
	t.server.Close()
}

// createNodeList is a utility function to create the basic node list which contains the node version/status.
func createNodeList(nodes []*TestNode) []node {
	list := make([]node, 0, len(nodes))
	for _, n := range nodes {
		list = append(list, node{Version: n.Version, Status: n.Status})
	}

	return list
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
