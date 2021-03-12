package cbrest

import "github.com/couchbase/tools-common/cbvalue"

// TestNodes is a readbility wrapper around a slice of test nodes.
type TestNodes []*TestNode

// TestNode encapsulates the options which can be used to configure a single node in a test cluster.
type TestNode struct {
	Version    cbvalue.Version
	Status     string
	Services   []Service
	SSL        bool
	AltAddress bool

	// NOTE: Overriding the nodes hostname should be done with caution since it will likely cause a bootstrap failure.
	// It is intended to be used in conjunction with a "normal" node to ensure the hostnames are correctly
	// updated/wrapped where required.
	OverrideHostname    []byte
	OverrideAltHostname []byte
}
