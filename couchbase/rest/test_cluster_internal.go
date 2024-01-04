package rest

import cbvalue "github.com/couchbase/tools-common/couchbase/v3/value"

// node is the structure used when marshalling basic node information.
type node struct {
	Version cbvalue.Version `json:"version"`
	Status  string          `json:"status"`
}

// vbsm represents the vBucketServerMap and is currently only used to indicate the number of vBuckets a bucket has.
type vbsm struct {
	VBucketMap [][2]int `json:"vBucketMap"`
}

// bucket is the structure used when marshalling basic bucket information some of which is configurable using the
// cluster options.
type bucket struct {
	Name string `json:"name"`
	UUID string `json:"uuid"`
	// ACSettings should be false, otherwise cbbackupmgr will try to decode the autocompaction settings.
	ACSettings       bool   `json:"autoCompactionSettings"`
	VBucketServerMap vbsm   `json:"VBucketServerMap"`
	Nodes            []node `json:"nodes"`
}
