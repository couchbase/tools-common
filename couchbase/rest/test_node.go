package rest

import cbvalue "github.com/couchbase/tools-common/couchbase/v3/value"

// TestNodes is a readbility wrapper around a slice of test nodes.
type TestNodes []*TestNode

// TestNode encapsulates the options which can be used to configure a single node in a test cluster.
//
// NOTE: Only one of 'Services' and 'ServicePorts' should be passed. If 'Services' is passed then the ports for those
// services are filled out with the test cluster's port.
type TestNode struct {
	Version      cbvalue.Version
	Status       string
	Services     []Service
	ServicePorts *Services
	SSL          bool
	AltAddress   bool
}
