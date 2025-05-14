// Package value exposes types related to Couchbase e.g. versions.
package value

import (
	"strings"

	"golang.org/x/mod/semver"
)

// ClusterVersion encapsulates version information for a Couchbase cluster, including whether or not it is operating in
// mixed mode.
type ClusterVersion struct {
	MinVersion Version `json:"min_version"`
	Mixed      bool    `json:"is_mixed_cluster"`
}

// Version represents a Couchbase Server or Columnar version and provides utilities for convenient comparison. Columnar
// versions are normalized to equivalent Couchbase Server versions to enable direct comparisons between the products.
type Version string

const (
	// VersionUnknown indicates the cluster is running an unknown version of Couchbase Server; this is usually a
	// development build and therefore is treated as being the latest version during comparisons.
	VersionUnknown = Version("0.0.0")

	// Version5_0_0 represents the 5.0.0 release of Couchbase Server (Spock).
	Version5_0_0 = Version("5.0.0")

	// Version5_5_0 represents the 5.5.0 release of Couchbase Server (Vulcan).
	Version5_5_0 = Version("5.5.0")

	// Version6_0_0 represents the 6.0.0 release of Couchbase Server (Alice).
	Version6_0_0 = Version("6.0.0")

	// Version6_5_0 represents the 6.5.0 release of Couchbase Server (Mad-Hatter).
	Version6_5_0 = Version("6.5.0")

	// Version6_6_0 represents the 6.6.0 release of Couchbase Server (Mad-Hatter).
	Version6_6_0 = Version("6.6.0")

	// Version7_0_0 represents the 7.0.0 release of Couchbase Server (Cheshire-Cat).
	Version7_0_0 = Version("7.0.0")

	// Version7_0_1 represents the 7.0.1 release of Couchbase Server (Cheshire-Cat).
	Version7_0_1 = Version("7.0.1")

	// Version7_0_2 represents the 7.0.2 release of Couchbase Server (Cheshire-Cat).
	Version7_0_2 = Version("7.0.2")

	// Version7_1_0 represents the 7.1.0 release of Couchbase Server (Neo).
	Version7_1_0 = Version("7.1.0")

	// Version7_2_0 represents the 7.2.0 release of Couchbase Server (also called Neo).
	Version7_2_0 = Version("7.2.0")

	// Version7_6_0 represents the 7.6.0 release of Couchbase Server (Trinity).
	Version7_6_0 = Version("7.6.0")

	// Version7_6_1 represents the 7.6.1 release of Couchbase Server (Trinity).
	Version7_6_1 = Version("7.6.1")

	// Version7_6_3 represents the 7.6.3 release of Couchbase Server (Trinity).
	Version7_6_3 = Version("7.6.3")

	// Version7_6_4 represents the 7.6.4 release of Couchbase Server (Trinity).
	Version7_6_4 = Version("7.6.4")

	// Version8_0_0 represents the 7.6.0 release of Couchbase Server (Morpheus).
	Version8_0_0 = Version("8.0.0")

	// VersionLatest represents the latest known version of Couchbase server, this may be an unreleased version.
	VersionLatest = Version8_0_0

	// VersionColumnarUnknown indicates the cluster is running an unknown version of Couchbase Columnar; this is
	// usually a development build and therefore is treated as being the latest version during comparisons.
	VersionColumnarUnknown = Version("0.0.0-columnar")

	// VersionColumnar1_0_0 represents the 1.0.0 release of Couchbase Columnar (Goldfish).
	VersionColumnar1_0_0 = Version("1.0.0-columnar")

	// VersionColumnar1_0_1 represents the 1.0.1 release of Couchbase Columnar (Goldfish).
	VersionColumnar1_0_1 = Version("1.0.1-columnar")

	// VersionColumnar1_0_2 represents the 1.0.2 release of Couchbase Columnar (Goldfish).
	VersionColumnar1_0_2 = Version("1.0.2-columnar")

	// VersionColumnar1_0_3 represents the 1.0.3 release of Couchbase Columnar (Goldfish).
	VersionColumnar1_0_3 = Version("1.0.3-columnar")

	// VersionColumnar1_0_5 represents the 1.0.5 release of Couchbase Columnar (Goldfish).
	VersionColumnar1_0_5 = Version("1.0.5-columnar")

	// VersionColumnar1_1_0 represents the 1.1.0 release of Couchbase Columnar (Ionic).
	VersionColumnar1_1_0 = Version("1.1.0-columnar")

	// VersionColumnarLatest represents the latest known version of Couchbase Columnar, this may be an unreleased version.
	VersionColumnarLatest = VersionColumnar1_1_0

	// VersionEnterpriseAnalytics2_0_0 represents the 2.0.0 release of Enterprise Analytics (Phoenix).
	VersionEnterpriseAnalytics2_0_0 = Version("2.0.0-enterprise-analytics")

	// VersionEnterpriseAnalyticsLatest represents the latest known version of Enterprise Analytics, this may be an
	// unreleased version.
	VersionEnterpriseAnalyticsLatest = VersionEnterpriseAnalytics2_0_0
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
	return semver.Compare("v"+string(v.ServerVersion()), "v"+string(other.ServerVersion()))
}

func (v Version) fixupMissingUnknown() Version {
	// note- this always returns the server (not columnar) latest in the event of missing version
	if v == "" || v == VersionUnknown {
		return VersionLatest
	} else if v == VersionColumnarUnknown {
		return VersionColumnarLatest
	}

	return v
}

// Equal returns a boolean indicating whether the current version is equal to the provided version.
func (v Version) Equal(other Version) bool {
	// semver.Compare() returns 0 if both versions are invalid
	return v.compare(other) == 0 && semver.IsValid("v"+string(v)) || v == other
}

// ServerVersion returns the Couchbase server version that this version equates to, after resolving unknown & missing
// versions.
func (v Version) ServerVersion() Version {
	v = v.fixupMissingUnknown()
	if !v.IsColumnar() {
		return v
	}

	switch v {
	case VersionColumnar1_0_0, VersionColumnar1_0_1:
		return Version7_6_1
	case VersionColumnar1_0_2:
		return Version7_6_3
	case VersionColumnar1_0_3, VersionColumnar1_0_5, VersionColumnar1_1_0:
		return Version7_6_4
	case VersionEnterpriseAnalytics2_0_0:
		return Version8_0_0
	default:
		return VersionUnknown
	}
}

// IsColumnar returns a boolean indicating whether this version represents a Couchbase Columnar product version.
func (v Version) IsColumnar() bool {
	return strings.HasSuffix(string(v), "-columnar") ||
		strings.HasSuffix(string(v), "-enterprise-analytics")
}

// ParseVersion parses the supplied product version string into a Version.
func ParseVersion(version string) Version {
	versionSplits := strings.Split(version, "-")
	suffix := versionSplits[len(versionSplits)-1]

	if suffix == "columnar" {
		return Version(versionSplits[0] + "-" + suffix)
	}

	return Version(versionSplits[0])
}
