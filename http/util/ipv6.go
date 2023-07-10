package util

import (
	"fmt"
	"strings"
)

// ReconstructIPV6 returns a new string where a valid unwrapped IPv6 address is wrapped with '[' and ']'.
func ReconstructIPV6(host string) string {
	if strings.Contains(host, ":") && (len(host) > 2 && host[0] != '[' && host[len(host)-1] != ']') {
		return fmt.Sprintf("[%s]", host)
	}

	return host
}
