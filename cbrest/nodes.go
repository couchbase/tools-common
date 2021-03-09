package cbrest

import (
	"encoding/json"
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

// UnmarshalJSON implements the JSON unmarshaler interface for a slice of nodes.
func (n *Nodes) UnmarshalJSON(data []byte) error {
	type overlay struct {
		NodeExt []*Node `json:"nodesExt"`
	}

	var decoded overlay

	err := json.Unmarshal(data, &decoded)
	if err != nil {
		return err
	}

	*n = decoded.NodeExt

	return nil
}

// Node encapsulates the addressing information for a single node in a Couchbase Cluster.
type Node struct {
	Hostname           string              `json:"hostname"`
	Services           *Services           `json:"services"`
	AlternateAddresses *AlternateAddresses `json:"alternateAddresses,omitempty"`
}

// Copy returns a deep copy of the the node.
func (n *Node) Copy() *Node {
	var services Services
	if n.Services != nil {
		services = *n.Services
	}

	var alternateAddresses AlternateAddresses
	if n.AlternateAddresses != nil {
		alternateAddresses = *n.AlternateAddresses
	}

	return &Node{
		Hostname:           n.Hostname,
		Services:           &services,
		AlternateAddresses: &alternateAddresses,
	}
}

// GetHostname returns the fully qualified hostname to this node whilst honoring whether to use ssl or
// alternative addressing.
//
// NOTE: Will return an empty string if there are no valid ports/addresses that we can use against this node.
func (n *Node) GetHostname(service Service, useSSL, useAltAddr bool) string {
	schema := "http"
	if useSSL {
		schema = "https"
	}

	hostname := n.hostname(useAltAddr)
	if hostname == "" {
		return ""
	}

	port := n.port(service, useSSL, useAltAddr)
	if port == 0 {
		return ""
	}

	return fmt.Sprintf("%s://%s:%d", schema, hostname, port)
}

// hostname returns the hostname which can be used to address this node whilst honoring alternative addressing.
//
// NOTE: Will return an empty string if this node doesn't have alternative addressing enabled.
func (n *Node) hostname(useAltAddr bool) string {
	if !useAltAddr {
		return n.Hostname
	}

	if n.AlternateAddresses == nil {
		return ""
	}

	return n.AlternateAddresses.Hostname
}

// port returns the port which will be used to address this node whilst honoring whether to use ssl and alternative
// addressing.
//
// NOTE: Will return a zero value port if no valid port can be found.
func (n *Node) port(service Service, useSSL, useAltAddr bool) uint16 {
	if !useAltAddr {
		return n.Services.GetPort(service, useSSL, useAltAddr)
	}

	if n.AlternateAddresses == nil {
		return 0
	}

	return n.AlternateAddresses.Services.GetPort(service, useSSL, useAltAddr)
}

// AlternateAddresses is similar to the 'Node' structure but encapsulates all the alternative addressing information.
type AlternateAddresses struct {
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
}

// GetPort returns the port which a request should be sent to whilst honoring whether to use ssl.
//
// NOTE: The 'useAltAddr' is solely used by 'views' because different ports are used when sending REST requests when
// alternative addressing is enabled.
func (s *Services) GetPort(service Service, useSSL, useAltAddr bool) uint16 {
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

		if useSSL && useAltAddr {
			return s.CAPISSL
		}

		if useSSL {
			return s.ManagementSSL
		}

		if useAltAddr {
			return s.CAPI
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
