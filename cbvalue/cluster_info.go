package cbvalue

// ClusterInfo encapsulates the information collected by the REST client after bootstrapping. We save this commonly used
// information to avoid multiple REST requests to the same endpoints (for the same information).
type ClusterInfo struct {
	// Retrieved via the /pools endpoint
	Enterprise bool   `json:"enterprise"`
	UUID       string `json:"uuid"`

	// Retrieved via the /pools/default endpoint
	Version ClusterVersion `json:"version"`

	// Retrieved via the /pools/default/buckets endpoint
	MaxVBuckets     uint16 `json:"max_vbuckets"`
	UniformVBuckets bool   `json:"uniform_vbuckets"`
}
