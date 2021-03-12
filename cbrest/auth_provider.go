package cbrest

import (
	"fmt"

	"github.com/couchbase/tools-common/auth"
	"github.com/couchbase/tools-common/connstr"
)

// AuthProvider is the auth provider for the REST client which handles providing credentials/hosts required to execute
// REST requests. This handles providing alternative addresses and SSL ports all of which should be completely
// transparent to the REST client itself.
type AuthProvider struct {
	resolved *connstr.ResolvedConnectionString

	index     int
	increment bool

	userAgent string
	username  string
	password  string

	nodes    Nodes
	mappings auth.HostMappings

	useAltAddr bool
}

// NewAuthProvider creates a new 'AuthProvider' using the provided credentials.
func NewAuthProvider(resolved *connstr.ResolvedConnectionString, username, password, userAgent string) *AuthProvider {
	return &AuthProvider{
		resolved:  resolved,
		username:  username,
		password:  password,
		userAgent: userAgent,
		mappings:  auth.GetHostMappings(),
	}
}

// GetServiceHost gets the host required to execute a REST request. A service may be provided to indicate that this
// request needs to be sent to a specific service.
//
// NOTE: The returned string is a fully qualified hostname with scheme and port.
func (a *AuthProvider) GetServiceHost(service Service) (string, error) {
	// If we haven't bootstrapped the client yet, return the next bootstrap address
	if len(a.nodes) == 0 {
		host := a.bootstrapHost()
		if host == "" {
			return "", errExhaustedBootstrapHosts
		}

		return host, nil
	}

	// Search through the nodes in the cluster for an appropriate node to send the request to
	for _, node := range a.nodes {
		hostname := node.GetHostname(service, a.resolved.UseSSL, a.useAltAddr)
		if hostname != "" {
			return hostname, nil
		}
	}

	// We weren't able to find any cluster nodes for the given service, error out
	return "", &ServiceNotAvailableError{service: service}
}

// GetAllServiceHosts gets all the possible hosts for a given service type.
//
// NOTE: The returned strings are fully qualified hostnames with schemes and ports.
func (a *AuthProvider) GetAllServiceHosts(service Service) ([]string, error) {
	// If we haven't bootstrapped the client yet, return the next bootstrap address. In theory this function should not
	// be used when bootstrapping; that should be handled by 'GetServiceHost'.
	if len(a.nodes) == 0 {
		host := a.bootstrapHost()
		if host == "" {
			return nil, errExhaustedBootstrapHosts
		}

		return []string{host}, nil
	}

	hosts := make([]string, 0)

	for _, node := range a.nodes {
		hostname := node.GetHostname(service, a.resolved.UseSSL, a.useAltAddr)
		if hostname != "" {
			hosts = append(hosts, hostname)
		}
	}

	// We didn't find any hosts for the given service
	if len(hosts) == 0 {
		return nil, &ServiceNotAvailableError{service: service}
	}

	return hosts, nil
}

// bootstrapHost returns the next node in the resolved connection string. This will be used to bootstrap the client i.e.
// fetch the list of nodes in the cluster.
func (a *AuthProvider) bootstrapHost() string {
	// We increment the boostrap address index before returning the next address. This means calls to 'GetFallbackHost'
	// and other similar functions will return the host for the node that we bootstrapped against.
	if a.increment {
		a.index++
	}

	if a.index >= len(a.resolved.Addresses) {
		return ""
	}

	a.increment = true

	scheme := "http"
	if a.resolved.UseSSL {
		scheme = "https"
	}

	return fmt.Sprintf("%s://%s:%d", scheme, a.resolved.Addresses[a.index].Host,
		a.resolved.Addresses[a.index].Port)
}

// GetFallbackHost returns the hostname for the bootstrap host. Used in the fallback case where a cluster node doesn't
// have a hostname.
func (a *AuthProvider) GetFallbackHost() string {
	return a.resolved.Addresses[a.index].Host
}

// GetCredentials returns the username/password credentials needed to authenicate against the given host.
func (a *AuthProvider) GetCredentials(host string) (string, string) {
	return auth.GetCredentials(a.username, a.password, host, a.mappings)
}

// GetUserAgent returns a string which should be used as the 'User-Agent' header of any REST requests.
func (a *AuthProvider) GetUserAgent() string {
	return a.userAgent
}

// SetNodes sets the list of nodes in the cluster to the one provided and determines if we should be using alternative
// addressing.
func (a *AuthProvider) SetNodes(nodes Nodes) {
	a.nodes = nodes
	a.useAltAddr = a.shouldUseAltAddr(a, nodes)
}

// shouldUseAltAddr returns a boolean indicating whether we should send all future requests using alternative addresses.
func (a *AuthProvider) shouldUseAltAddr(credentials *AuthProvider, nodes Nodes) bool {
	for _, node := range nodes {
		if node.Hostname == credentials.resolved.Addresses[a.index].Host {
			return false
		}

		if node.AlternateAddresses.External != nil &&
			node.AlternateAddresses.External.Hostname == credentials.resolved.Addresses[a.index].Host {
			return true
		}
	}

	return false
}
