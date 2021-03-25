package cbrest

import "encoding/json"

// TestBuckets is a readbility alias around a map of buckets.
type TestBuckets map[string]*TestBucket

// TestBucket represents a bucket that will exist in the test cluster. These attributes will configure the values
// returned by REST requests to the cluster.
type TestBucket struct {
	UUID        string
	NumVBuckets uint16
	// Manifest should be a '*collections.Manifest' from 'backup'.
	Manifest json.Marshaler
}
