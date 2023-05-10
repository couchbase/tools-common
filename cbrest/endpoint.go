package cbrest

import (
	"github.com/couchbase/tools-common/httptools"
)

const (
	// EndpointPools is the root of the 'ns_server' REST API. Used to fetch version information from the cluster.
	EndpointPools httptools.Endpoint = "/pools"

	// EndpointPoolsDefault represents the default cluster (i.e. 'self'). Used for extracting information about the
	// cluster itself.
	EndpointPoolsDefault httptools.Endpoint = "/pools/default"

	// EndpointBuckets represents the 'ns_server' endpoint used to interact with the buckets on the cluster.
	EndpointBuckets httptools.Endpoint = "/pools/default/buckets"

	// EndpointBucket represents the endpoint for interacting with a specific named bucket.
	EndpointBucket httptools.Endpoint = "/pools/default/buckets/%s"

	// EndpointBucketManifest represents the bucket collections manifest endpoint, can be used to get/update the
	// collection manifest for a bucket.
	EndpointBucketManifest httptools.Endpoint = "/pools/default/buckets/%s/scopes"

	// EndpointNodesServices is used during the bootstrapping process to fetch a list of all the nodes in the cluster.
	EndpointNodesServices httptools.Endpoint = "/pools/default/nodeServices"
)
