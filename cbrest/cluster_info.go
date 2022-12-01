package cbrest

import "fmt"

// clusterInfo encapsulates the information collected by the REST client after bootstrapping. We save this commonly used
// information to avoid multiple REST requests to the same endpoints (for the same information).
type clusterInfo struct {
	// Retrieved via the /pools endpoint
	Enterprise       bool   `json:"enterprise,omitempty"`
	UUID             string `json:"uuid,omitempty"`
	DeveloperPreview bool   `json:"developer_preview,omitempty"`
}

// getClusterInfo gets commonly used information about the cluster; this includes the uuid and version.
func (c *Client) getClusterInfo() (*clusterInfo, error) {
	meta, err := c.getClusterMetaData()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster metadata: %w", err)
	}

	info := clusterInfo{
		Enterprise:       meta.Enterprise,
		UUID:             meta.UUID,
		DeveloperPreview: meta.DeveloperPreview,
	}

	return &info, nil
}
