package netutil

import "strings"

// ShouldReconstructIPV6 returns a boolean indicating whether the given host is an IPV6 address without surrounding
// brackets and should be reconstructed.
func ShouldReconstructIPV6(host string) bool {
	return strings.Contains(host, ":") && (len(host) > 2 && host[0] != '[' && host[len(host)-1] != ']')
}
