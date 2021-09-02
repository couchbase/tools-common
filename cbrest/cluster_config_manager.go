package cbrest

import (
	"context"
	"sync"
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

// BootstrapNode returns the node which we bootstrapped against.
func (c *ClusterConfig) BootstrapNode() *Node {
	for _, node := range c.Nodes {
		if node.BootstrapNode {
			return node
		}
	}

	// Our unit testing currently doesn't set the 'BootstrapNode' value, use a sane value
	return c.Nodes[0]
}

// FilterOtherNodes filters out all nodes which aren't the bootstrap node; this has the effect of forcing all
// communication via the bootstrap node.
func (c *ClusterConfig) FilterOtherNodes() {
	c.Nodes = Nodes{c.BootstrapNode()}
}

// ClusterConfigManager is a utility wrapper around the current cluster config which provides utility functions required
// when periodically updating the REST clients cluster config.
type ClusterConfigManager struct {
	config *ClusterConfig
	last   *time.Time
	maxAge time.Duration

	// Related to triggering config updates in-between request retries
	cond   *sync.Cond
	signal chan struct{}
	once   sync.Once
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
		cond:   sync.NewCond(&sync.Mutex{}),
		signal: make(chan struct{}),
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
	c.cond.L.Lock()

	defer func() {
		c.cond.Broadcast()
		c.cond.L.Unlock()
	}()

	if c.config != nil && c.config.Revision > config.Revision {
		return &OldClusterConfigError{old: config.Revision, curr: c.config.Revision}
	}

	now := time.Now()

	c.config = config
	c.last = &now

	return nil
}

// WaitUntilUpdated triggers a config update and then blocks the calling goroutine until the update is complete.
func (c *ClusterConfigManager) WaitUntilUpdated(ctx context.Context) {
	signal := make(chan struct{})
	go c.closeWhenUpdated(signal)

	select {
	case <-ctx.Done():
	case <-signal:
	}
}

// closeWhenUpdated closes the provided signal channel once the cluster config has been successfully updated.
func (c *ClusterConfigManager) closeWhenUpdated(signal chan struct{}) {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	// Wake the CCP goroutine to trigger updating the cluster config
	c.once.Do(func() {
		close(c.signal)
	})

	// Unlock and wait for the CCP goroutine to have updated the cluster config
	c.cond.Wait()

	// Wake up the calling goroutine, we now have the most up-to-date config (note that it may be the same revision)
	close(signal)
}

// WaitUntilExpired blocks the calling goroutine until the current config has expired and the client should update the
// cluster config.
func (c *ClusterConfigManager) WaitUntilExpired(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-c.createSignalChannel():
	case <-time.After(time.Until(c.last.Add(c.maxAge))):
	}
}

// createSignalChannel creates a new signal channel which can be used to wake up the CCP goroutine to trigger a cluster
// config update.
func (c *ClusterConfigManager) createSignalChannel() chan struct{} {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	// Recreate the signal channel and its accompanying cleanup once. We use a channel and a once in conjunction to
	// allow multiple goroutines to call 'WaitUntilUpdated' concurrently whilst avoiding causing successive updates to
	// the cluster config.
	c.signal = make(chan struct{})
	c.once = sync.Once{}

	return c.signal
}
