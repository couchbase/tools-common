package cbrest

import (
	"fmt"
	"sync"

	"github.com/couchbase/tools-common/aprov"
	"github.com/couchbase/tools-common/connstr"
)

// AuthProvider is the auth provider for the REST client which handles providing credentials/hosts required to execute
// REST requests. This handles providing alternative addresses and SSL ports all of which should be completely
// transparent to the REST client itself.
type AuthProvider struct {
	resolved *connstr.ResolvedConnectionString

	useAltAddr bool

	provider aprov.Provider

	manager *ClusterConfigManager
	lock    sync.RWMutex
}

// NewAuthProvider creates a new 'AuthProvider' using the provided credentials.
func NewAuthProvider(resolved *connstr.ResolvedConnectionString, provider aprov.Provider) *AuthProvider {
	return &AuthProvider{
		resolved: resolved,
		provider: provider,
		manager:  NewClusterConfigManager(),
	}
}

// GetServiceHost gets the host required to execute a REST request. A service may be provided to indicate that this
// request needs to be sent to a specific service.
//
// Supplying an offest will (where possible) "shift" the node index so that we dispatch the request to a different node;
// this may help in certain cases where a node is currently being removed from the cluster.
//
// NOTE: The returned string is a fully qualified hostname with scheme and port.
func (a *AuthProvider) GetServiceHost(service Service, offset int) (string, error) {
	hosts, err := a.GetAllServiceHosts(service)
	if err != nil {
		return "", err // Purposefully not wrapped
	}

	// If the bootstrap host is running the required service, it will be placed at the beginning of the slice by the
	// 'GetAllServiceHosts' function; this means we prioritize sending requests to the node which we bootstrapped
	// against.
	return hosts[offset%len(hosts)], nil
}

// GetAllServiceHosts gets all the possible hosts for a given service type.
//
// NOTE: The returned strings are fully qualified hostnames with schemes and ports.
func (a *AuthProvider) GetAllServiceHosts(service Service) ([]string, error) {
	a.lock.RLock()
	defer a.lock.RUnlock()

	config := a.manager.GetClusterConfig()

	// We've not bootstrapped the client yet, this shouldn't happen in the normal case for the REST client since we
	// bootstrap upon creation.
	if config == nil {
		return nil, ErrNotBootstrapped
	}

	hosts := make([]string, 0)

	for _, node := range config.Nodes {
		hostname, bootstrap := node.GetQualifiedHostname(service, a.resolved.UseSSL, a.useAltAddr)
		if hostname == "" {
			continue
		}

		if bootstrap {
			hosts = append([]string{hostname}, hosts...)
		} else {
			hosts = append(hosts, hostname)
		}
	}

	// We didn't find any hosts for the given service
	if len(hosts) == 0 {
		return nil, &ServiceNotAvailableError{service: service}
	}

	return hosts, nil
}

// GetCredentials returns the username/password credentials needed to authenticate against the given host.
func (a *AuthProvider) GetCredentials(host string) (string, string) {
	return a.provider.GetCredentials(host)
}

// GetUserAgent returns a string which should be used as the 'User-Agent' header of any REST requests.
func (a *AuthProvider) GetUserAgent() string {
	return a.provider.GetUserAgent()
}

// SetClusterConfig updates the auth providers cluster config in a thread safe fashion. Returns an error if the provided
// config is older than the current config; this ensures that we don't use the config from a node which have been
// removed from the cluster.
func (a *AuthProvider) SetClusterConfig(host string, config *ClusterConfig) error {
	a.lock.Lock()
	defer a.lock.Unlock()

	if err := a.manager.Update(config); err != nil {
		return err
	}

	// Only update the alternate address settings if we've accepted the config
	a.useAltAddr = a.shouldUseAltAddr(host, config.Nodes)

	return nil
}

// bootstrapHostFunc returns a function which, when called successively will return a hostname which can be used to
// attempt to bootstrap the client against.
//
// NOTE: The returned closure will return an empty string once all the addresses in the resolved connection string have
// been exhausted.
func (a *AuthProvider) bootstrapHostFunc() func() string {
	var index int

	return func() string {
		defer func() { index++ }()

		if index >= len(a.resolved.Addresses) {
			return ""
		}

		scheme := "http"
		if a.resolved.UseSSL {
			scheme = "https"
		}

		return fmt.Sprintf("%s://%s:%d", scheme, a.resolved.Addresses[index].Host,
			a.resolved.Addresses[index].Port)
	}
}

// shouldUseAltAddr returns a boolean indicating whether we should send all future requests using alternative addresses.
func (a *AuthProvider) shouldUseAltAddr(host string, nodes Nodes) bool {
	for _, node := range nodes {
		if node.Hostname == host {
			return false
		}

		if node.AlternateAddresses.External != nil && node.AlternateAddresses.External.Hostname == host {
			return true
		}
	}

	return false
}
