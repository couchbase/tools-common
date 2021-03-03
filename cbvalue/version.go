package cbvalue

import "strings"

// ClusterVersion encapsulates version information for a Couchbase cluster, including whether or not it is operating in
// mixed mode.
type ClusterVersion struct {
	MinVersion Version `json:"min_version"`
	Mixed      bool    `json:"is_mixed_cluster"`
}

// Version represents a Couchbase Server version and provides utilities for convenient comparison.
type Version string

const (
	// VersionUnknown indicates the cluster is running an unknown version of Couchbase Server; this is usually a
	// development build and therefore is treated as being the latest version during comparisons.
	VersionUnknown = Version("0.0.0")

	// Version5_0_0 represents the 5.0.0 release of Couchbase Server.
	Version5_0_0 = Version("5.0.0")

	// Version5_5_0 represents the 5.5.0 release of Couchbase Server.
	Version5_5_0 = Version("5.5.0")

	// Version6_0_0 represents the 6.0.0 release of Couchbase Server.
	Version6_0_0 = Version("6.0.0")

	// Version6_5_0 represents the 6.5.0 release of Couchbase Server.
	Version6_5_0 = Version("6.5.0")

	// Version6_6_0 represents the 6.6.0 release of Couchbase Server.
	Version6_6_0 = Version("6.6.0")

	// Version7_0_0 represents the 7.0.0 release of Couchbase Server.
	Version7_0_0 = Version("7.0.0")

	// VersionLatest represents the latest known version of Couchbase server, this may be an unreleased version.
	VersionLatest = Version7_0_0
)

// Older returns a boolean indicating whether the current version is older than the provided version.
//
// NOTE: The unknown version is a special case and is treated as the latest version.
func (v Version) Older(other Version) bool {
	return v.compare(other) < 0
}

// Newer returns a boolean indicating whether the current version is newer than the provided version.
//
// NOTE: The unknown version is a special case and is treated as the latest version.
func (v Version) Newer(other Version) bool {
	return v.compare(other) > 0
}

// AtLeast returns a boolean indicating whether the current version is higher than or equal to the provided version.
//
// NOTE: The unknown version is a special case and is treated as the latest version.
func (v Version) AtLeast(other Version) bool {
	return v.compare(other) >= 0
}

// compare is a utility function which performs a string comparison of the provided version whilst specifically handing
// the case where the versions are empty/unknown.
func (v Version) compare(other Version) int {
	if v == "" || v == VersionUnknown {
		v = VersionLatest
	}

	if other == "" || other == VersionUnknown {
		other = VersionLatest
	}

	return strings.Compare(string(v), string(other))
}

// Equal returns a boolean indicating whether the current version is equal to the provided version.
func (v Version) Equal(other Version) bool {
	return string(v) == string(other)
}
