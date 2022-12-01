package cbrest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// clusterMetadata wraps some common cluster metadata.
type clusterMetadata struct {
	Enterprise       bool   `json:"isEnterprise"`
	UUID             string `json:"uuid"`
	DeveloperPreview bool   `json:"isDeveloperPreview"`
}

// getClusterMetaData extracts some common metadata from the cluster.
func (c *Client) getClusterMetaData() (*clusterMetadata, error) {
	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           EndpointPools,
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	response, err := c.Execute(request)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	var decoded *clusterMetadata

	err = json.Unmarshal(response.Body, &decoded)
	if err == nil {
		return decoded, nil
	}

	// We will fail to unmarshal the response from the node if it's uninitialized, this is because the "uuid" field will
	// be an empty array, instead of a string; if this is the case, return a clearer error message.
	if bytes.Contains(response.Body, []byte(`"uuid":[]`)) {
		return nil, ErrNodeUninitialized
	}

	return nil, fmt.Errorf("failed to unmarshal response: %w", err)
}
