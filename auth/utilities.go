package auth

import (
	"github.com/couchbase/tools-common/log"
	"github.com/couchbase/tools-common/netutil"

	"github.com/couchbase/cbauth"
)

// GetCredentials return the credentials for the current user/endpoint. There are three different behaviours which are
// currently supported:
// 1) When we're using the '@ns_server' user, we use cbauth to fetch the correct username/password.
// 2) When we're using the '@backup' user, we use the 'HostMappings' to fetch the correct username/password.
// 3) When we're acting as another user, return the provided username/password.
func GetCredentials(username, password, endpoint string, mappings HostMappings) (string, string) {
	switch username {
	case NSServerUser:
		_, password, err := cbauth.GetHTTPServiceAuth(netutil.TrimSchema(endpoint))
		if err == nil {
			return NSServerUser, password
		}

		log.Warnf("(Auth) (cbauth) Unknown host '%s', will fallback to local credentials", endpoint)
	case BackupServiceUser:
		password, err := mappings.GetPassword(endpoint)
		if err == nil {
			return BackupServiceUser, password
		}

		log.Warnf("(Auth) (Service) Unknown host '%s', will fallback to local credentials", endpoint)
	}

	return username, password
}
