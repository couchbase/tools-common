package rest

import cbvalue "github.com/couchbase/tools-common/couchbase/value"

// TestNodes is a readbility wrapper around a slice of test nodes.
type TestNodes []*TestNode

// TestNode encapsulates the options which can be used to configure a single node in a test cluster.
type TestNode struct {
	Version    cbvalue.Version
	Status     string
	Services   []Service
	SSL        bool
	AltAddress bool
}
