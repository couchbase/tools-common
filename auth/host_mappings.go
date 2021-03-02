package auth

import (
	"fmt"
	"os"
	"strings"

	"github.com/couchbase/tools-common/log"
	"github.com/couchbase/tools-common/netutil"
)

const (
	// BackupServiceUser is the user which will be supplied if the backup service is running the given command.
	BackupServiceUser = "@backup"

	// NSServerUser is the user which will be supplied if ns_server is running the given command.
	NSServerUser = "@ns_server"
)

// localhosts is a slice of known aliases to localhost.
var localhosts = []string{"localhost:", "127.0.0.1:", "[::1]:"}

// HostMappings - Represents mappings between hosts:ports to passwords. Mappings are provided by the Backup service
// during a backup to allow 'cbbackupmgr' to authenticate on each node in the cluster.
type HostMappings map[string]string

// GetHostMappings reads the environmental variable CBM_SERVICES_KV_HOSTS which should be a comma-separated list of the
// form host:port=password,host2:port2=password2. This represents the passwords for the special user for each endpoint.
func GetHostMappings() HostMappings {
	hosts := make(HostMappings)

	val, ok := os.LookupEnv("CBM_SERVICES_KV_HOSTS")
	if !ok {
		return hosts
	}

	hostParts := strings.Split(val, ",")
	for _, hostAndPassword := range hostParts {
		split := strings.Split(hostAndPassword, "=")
		if len(split) != 2 {
			log.Errorf("(Service) Invalid entry in services host map %s", hostAndPassword)
			continue
		}

		hosts[split[0]] = split[1]
	}

	return hosts
}

// GetPassword looks for a matching host in the list and returns the password, if not found it returns an error.
func (h HostMappings) GetPassword(host string) (string, error) {
	var (
		hostToCheck = []string{host}
		isLocalHost string
	)

	host = netutil.TrimSchema(host)

	for _, prefix := range localhosts {
		if strings.HasPrefix(host, prefix) {
			isLocalHost = prefix
			break
		}
	}

	if isLocalHost != "" {
		for _, prefix := range localhosts {
			if prefix == isLocalHost {
				continue
			}

			tempHost := host
			hostToCheck = append(hostToCheck, strings.Replace(tempHost, isLocalHost, prefix, 1))
		}
	}

	for _, checkHost := range hostToCheck {
		p, ok := h[checkHost]
		if ok {
			return p, nil
		}
	}

	return "", fmt.Errorf("unknown host: %s", host)
}
