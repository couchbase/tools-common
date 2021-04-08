package cbrest

import (
	"context"
	"time"

	"github.com/couchbase/tools-common/envvar"
	"github.com/couchbase/tools-common/log"
)

// DefaultCCMaxAge is the maximum amount of time a cluster config will be in use before the client begins to try to
// update the cluster config. This is around the same time used by the SDKS +/- 5 seconds.
const DefaultCCMaxAge = 15 * time.Second

// ClusterConfig represents the payload sent by 'ns_server' when hitting the '/pools/default/nodeServices' endpoint.
type ClusterConfig struct {
	Revision int64 `json:"rev"`
	Nodes    Nodes `json:"nodesExt"`
}

// ClusterConfigManager is a utility wrapper around the current cluster config which provides utility functions required
// when periodically updating the REST clients cluster config.
type ClusterConfigManager struct {
	config *ClusterConfig
	last   *time.Time
	maxAge time.Duration
}

// NewClusterConfigManager returns a new cluster config manager which will not immediately trigger a cluster config
// update.
func NewClusterConfigManager() *ClusterConfigManager {
	maxAge, ok := envvar.GetDuration("CB_REST_CC_MAX_AGE")
	if !ok {
		maxAge = DefaultCCMaxAge
	} else {
		log.Infof("(REST) (Cluster Config Manager) Set max cluster config age to: %s", maxAge)
	}

	now := time.Now()

	return &ClusterConfigManager{
		last:   &now,
		maxAge: maxAge,
	}
}

// GetClusterConfig returns a copy of the current cluster config. This method should be preferred over manually
// accessing the cluster config to avoid unexpected modifications.
func (c *ClusterConfigManager) GetClusterConfig() *ClusterConfig {
	if c.config == nil {
		return nil
	}

	return &ClusterConfig{Revision: c.config.Revision, Nodes: c.config.Nodes.Copy()}
}

// Update attempts to update the cluster config using the one provided, note that it may be rejected depending on the
// revision id.
func (c *ClusterConfigManager) Update(config *ClusterConfig) error {
	if c.config != nil && c.config.Revision > config.Revision {
		return &OldClusterConfigError{old: config.Revision, curr: c.config.Revision}
	}

	now := time.Now()

	c.config = config
	c.last = &now

	return nil
}

// Wait blocks the calling goroutine until the current config has expired and the client should update the cluster
// config.
func (c *ClusterConfigManager) Wait(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(time.Until(c.last.Add(c.maxAge))):
	}
}
