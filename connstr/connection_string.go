package connstr

import (
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/couchbase/tools-common/slice"
)

const (
	// DefaultHTTPPort is the default http management port for Couchbase Server. Will be used when no port is supplied
	// when using a non-ssl scheme.
	DefaultHTTPPort = 8091

	// DefaultHTTPSPort is the default https management port for Couchbase Server. Will be used when no port is supplied
	// when using a ssl scheme.
	DefaultHTTPSPort = 18091
)

// Address represents an address used to connect to Couchbase Server. Should not be used directly i.e. access should be
// provided through a 'ConnectionString' or a 'ResolvedConnectionString'.
type Address struct {
	// Host is a IP/DNS hostname.
	Host string

	// Port is a port number.
	//
	// NOTE: May be zero if omitted by the user.
	Port uint16
}

// ConnectionString represents a connection string that can be supplied to the 'backup' tools to give the tools a node
// or nodes in a cluster to bootstrap from.
type ConnectionString struct {
	// Scheme is the parsed scheme without '://'.
	Scheme string

	// Addresses represents the node addresses, this value must be non-empty.
	//
	// NOTE: This attribute is required, and must be non-empty.
	Addresses []Address

	// Params are any parsed query parameters, will be <nil> if none were parsed.
	Params url.Values
}

// ResolvedConnectionString is similar to a 'ConnectionString', however, addresses are resolved i.e. ports/schemes are
// converted into something that we can use to bootstrap from.
//
// NOTE: If provided with a valid srv record (an address with the scheme 'couchbase' or 'couchbases', and no port). This
// function will lookup the srv record and use those addresses.
type ResolvedConnectionString struct {
	// UseSSL indicates whether SSL should be used when connecting to the hosts.
	UseSSL bool

	// Addresses represents the resolved addresses for the cluster nodes.
	//
	// NOTE: This attribute is required, and must be non-empty.
	Addresses []Address

	// Params are any parsed query parameters, will be <nil> if none were parsed.
	Params url.Values
}

// Parse the given connection string and perform first tier validation i.e. it's possible for a parsed connection string
// to fail when the 'Resolve' function is called.
//
// For more information on the connection string formats accepted by this function, refer to the host formats
// documentation at https://docs.couchbase.com/server/7.0/backup-restore/cbbackupmgr-backup.html#host-formats.
func Parse(connectionString string) (*ConnectionString, error) {
	// partMatcher matches and groups the different parts of a given connection string. For example:
	// couchbases://10.0.0.1:11222,10.0.0.2,10.0.0.3:11207?network=external
	// Group 'scheme': couchbases
	// Group 'hosts': 10.0.0.1:11222,10.0.0.2,10.0.0.3:11207
	// Group 'params': network=external
	partMatcher := regexp.MustCompile(
		`((?P<scheme>.*):\/\/)?(([^\/?:]*)(:([^\/?:@]*))?@)?(?P<hosts>[^\/?]*)(\/([^\?]*))?(\?(?P<params>.*))?`,
	)

	// hostMatcher matches and groups different parts of a comma separated list of hostname in a connection string.
	// For example:
	// 10.0.0.1:11222,10.0.0.2,10.0.0.3:11207
	// Match 1:
	//   Group 'host': 10.0.0.1
	//   Group 'port': 11222
	// Match 2:
	//   Group 'host': 10.0.0.2
	//   Group 'port':
	// Match 3:
	//   Group 'host': 10.0.0.3
	//   Group 'port': 11207
	hostMatcher := regexp.MustCompile(`(?P<host>(\[[^\]]+\]+)|([^;\,\:]+))(:(?P<port>[0-9]*))?(;\,)?`)

	parts := partMatcher.FindStringSubmatch(connectionString)
	if parts == nil {
		return nil, ErrInvalidConnectionString
	}

	parsed := &ConnectionString{
		Scheme: parts[partMatcher.SubexpIndex("scheme")],
	}

	if !slice.ContainsString([]string{"", "http", "https", "couchbase", "couchbases"}, parsed.Scheme) {
		return nil, ErrBadScheme
	}

	// We don't need to check if 'FindAllStringSubmatch' returns <nil> since ranging over a <nil> slice results in no
	// iterations. We will then return an 'ErrNoAddressesParsed' error which is more informative than
	// 'ErrInvalidConnectionString'.
	for _, hostInfo := range hostMatcher.FindAllStringSubmatch(parts[partMatcher.SubexpIndex("hosts")], -1) {
		address, err := parseHost(hostInfo, hostMatcher)
		if err != nil {
			return nil, err
		}

		parsed.Addresses = append(parsed.Addresses, address)
	}

	if len(parsed.Addresses) == 0 {
		return nil, ErrNoAddressesParsed
	}

	var err error

	parsed.Params, err = parseParams(parts[partMatcher.SubexpIndex("params")])
	if err != nil {
		return nil, err
	}

	return parsed, nil
}

// parseHost extracts information from the given regex match returning the parsed address.
func parseHost(hostInfo []string, hostMatcher *regexp.Regexp) (Address, error) {
	address := Address{
		Host: hostInfo[hostMatcher.SubexpIndex("host")],
	}

	port := hostInfo[hostMatcher.SubexpIndex("port")]
	if port == "" {
		return address, nil
	}

	parsed, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return Address{}, ErrBadPort
	}

	address.Port = uint16(parsed)

	return address, nil
}

// parseParams parses and converts the given query parameter string.
func parseParams(params string) (url.Values, error) {
	parsed, err := url.ParseQuery(params)
	if err != nil {
		return nil, err
	}

	if len(parsed) == 0 {
		return nil, nil
	}

	return parsed, nil
}

// Resolve the current connection string and return addresses which can be used to bootstrap from. Will perform
// additional validation, once resolved the connection string is valid and can be used.
func (c *ConnectionString) Resolve() (*ResolvedConnectionString, error) {
	var (
		defaultPort uint16
		resolved    = &ResolvedConnectionString{Params: c.Params}
	)

	switch c.Scheme {
	case "http", "couchbase":
		defaultPort = DefaultHTTPPort
	case "https", "couchbases":
		defaultPort = DefaultHTTPSPort
		resolved.UseSSL = true
	case "":
		defaultPort = DefaultHTTPPort
	default:
		return nil, ErrBadScheme
	}

	if resolved := c.resolveSRV(); resolved != nil {
		return resolved, nil
	}

	resolvedDefault := DefaultHTTPPort
	if resolved.UseSSL {
		resolvedDefault = DefaultHTTPSPort
	}

	for _, address := range c.Addresses {
		resolvedAddress := Address{
			Host: address.Host,
			Port: address.Port,
		}

		if address.Port == 0 || address.Port == defaultPort || address.Port == DefaultHTTPPort {
			resolvedAddress.Port = uint16(resolvedDefault)
		}

		resolved.Addresses = append(resolved.Addresses, resolvedAddress)
	}

	if len(resolved.Addresses) == 0 {
		return nil, ErrNoAddressesResolved
	}

	return resolved, nil
}

// resolveSRV attempts to resolve the connection string as an srv record, the resulting connection string will be
// non-nil if it was a valid srv record containing one or more addresses.
func (c *ConnectionString) resolveSRV() *ResolvedConnectionString {
	validScheme := func(scheme string) bool {
		return scheme == "couchbase" || scheme == "couchbases"
	}

	validHostnameNoIP := func(addresses []Address) bool {
		return len(addresses) == 1 && addresses[0].Port == 0
	}

	validIP := func(address string) bool {
		return strings.Contains(address, ":") || net.ParseIP(address) != nil
	}

	if !validScheme(c.Scheme) || !validHostnameNoIP(c.Addresses) || validIP(c.Addresses[0].Host) {
		return nil
	}

	_, servers, err := net.LookupSRV(c.Scheme, "tcp", c.Addresses[0].Host)
	if err != nil || len(servers) <= 0 {
		return nil
	}

	resolved := &ResolvedConnectionString{
		UseSSL: c.Scheme == "couchbases",
	}

	for _, server := range servers {
		resolved.Addresses = append(resolved.Addresses, Address{
			Host: strings.TrimSuffix(server.Target, "."),
			Port: srvPort(c.Scheme),
		})
	}

	return resolved
}
