package rest

import (
	"fmt"
)

// Nodes is a wrapper around a slice of nodes which allows custom unmarshalling.
type Nodes []*Node

// Copy returns a deep copy of the slice of nodes.
func (n Nodes) Copy() Nodes {
	nodes := make(Nodes, 0, len(n))
	for _, node := range n {
		nodes = append(nodes, node.Copy())
	}

	return nodes
}

// Node encapsulates the addressing information for a single node in a Couchbase Cluster.
type Node struct {
	Hostname           string             `json:"hostname"`
	Services           *Services          `json:"services"`
	AlternateAddresses AlternateAddresses `json:"alternateAddresses"`
	BootstrapNode      bool               `json:"thisNode"`
}

// Copy returns a deep copy of the the node.
func (n *Node) Copy() *Node {
	var services Services
	if n.Services != nil {
		services = *n.Services
	}

	var external External
	if n.AlternateAddresses.External != nil {
		external = *n.AlternateAddresses.External
	}

	return &Node{
		Hostname:           n.Hostname,
		Services:           &services,
		AlternateAddresses: AlternateAddresses{External: &external},
		BootstrapNode:      n.BootstrapNode,
	}
}

// GetQualifiedHostname returns the fully qualified hostname to this node whilst honoring whether to use ssl or
// alternative addressing.
//
// NOTE: Will return an empty string if there are no valid ports/addresses that we can use against this node.
func (n *Node) GetQualifiedHostname(service Service, useSSL, useAltAddr bool) (string, bool) {
	schema := "http"
	if useSSL {
		schema = "https"
	}

	hostname := n.GetHostname(useAltAddr)
	if hostname == "" {
		return "", n.BootstrapNode
	}

	port := n.GetPort(service, useSSL, useAltAddr)
	if port == 0 {
		return "", n.BootstrapNode
	}

	return fmt.Sprintf("%s://%s:%d", schema, hostname, port), n.BootstrapNode
}

// GetHostname returns the hostname which can be used to address this node whilst honoring alternative addressing.
//
// NOTE: Will return an empty string if this node doesn't have alternative addressing enabled.
func (n *Node) GetHostname(useAltAddr bool) string {
	if !useAltAddr {
		return n.Hostname
	}

	if n.AlternateAddresses.External == nil {
		return ""
	}

	return n.AlternateAddresses.External.Hostname
}

// GetPort returns the port which will be used to address this node whilst honoring whether to use ssl and alternative
// addressing.
//
// NOTE: Will return a zero value port if no valid port can be found.
func (n *Node) GetPort(service Service, useSSL, useAltAddr bool) uint16 {
	if !useAltAddr {
		return n.Services.GetPort(service, useSSL)
	}

	if n.AlternateAddresses.External == nil {
		return 0
	}

	return n.AlternateAddresses.External.Services.GetPort(service, useSSL)
}

// AlternateAddresses represents the 'alternateAddresses' payload sent the 'nodeServices' endpoint.
type AlternateAddresses struct {
	External *External `json:"external"`
}

// External is similar to the 'Node' structure but encapsulates all the alternative addressing information.
type External struct {
	Hostname string    `json:"hostname"`
	Services *Services `json:"ports"`
}

// Services encapsulates the ports that are active on this cluster node.
type Services struct {
	CAPI              uint16 `json:"capi"`
	CAPISSL           uint16 `json:"capiSSL"`
	KV                uint16 `json:"kv"`
	KVSSL             uint16 `json:"kvSSL"`
	Management        uint16 `json:"mgmt"`
	ManagementSSL     uint16 `json:"mgmtSSL"`
	FullText          uint16 `json:"fts"`
	FullTextSSL       uint16 `json:"ftsSSL"`
	SecondaryIndex    uint16 `json:"indexHttp"`
	SecondaryIndexSSL uint16 `json:"indexHttps"`
	N1QL              uint16 `json:"n1ql"`
	N1QLSSL           uint16 `json:"n1qlSSL"`
	Eventing          uint16 `json:"eventingAdminPort"`
	EventingSSL       uint16 `json:"eventingSSL"`
	CBAS              uint16 `json:"cbas"`
	CBASSSL           uint16 `json:"cbasSSL"`
	Backup            uint16 `json:"backupAPI"`
	BackupSSL         uint16 `json:"backupAPIHTTPS"`
	ContBackupGRPC    uint16 `json:"contBackupGRPC"`
}

// GetPort returns the port which a request should be sent to whilst honoring whether to use ssl.
func (s *Services) GetPort(service Service, useSSL bool) uint16 {
	switch service {
	case ServiceManagement:
		if useSSL {
			return s.ManagementSSL
		}

		return s.Management
	case ServiceAnalytics:
		if useSSL {
			return s.CBASSSL
		}

		return s.CBAS
	case ServiceData:
		if useSSL {
			return s.KVSSL
		}

		return s.KV
	case ServiceEventing:
		if useSSL {
			return s.EventingSSL
		}

		return s.Eventing
	case ServiceGSI:
		if useSSL {
			return s.SecondaryIndexSSL
		}

		return s.SecondaryIndex
	case ServiceQuery:
		if useSSL {
			return s.N1QLSSL
		}

		return s.N1QL
	case ServiceSearch:
		if useSSL {
			return s.FullTextSSL
		}

		return s.FullText
	case ServiceViews:
		// Return a zero value port if this node is not running the Data Service, this has the affect of forcing the
		// request to be routed to a node which is running the Data Service.
		if (!useSSL && s.KV == 0) || (useSSL && s.KVSSL == 0) {
			return 0
		}

		if useSSL {
			return s.ManagementSSL
		}

		return s.Management
	case ServiceBackup:
		if useSSL {
			return s.BackupSSL
		}

		return s.Backup
	}

	// NOTE: This is a development time error, all requests should be being sent to known the service types which are
	// constant. In the event that we add a new service and don't update this function, we want to know about it as
	// quickly as possible.
	panic(fmt.Sprintf("unknown service '%s'", service))
}
