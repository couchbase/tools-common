package objval

import (
	"fmt"
)

// CloudProvider is an enumeration representing the possible locations that we can access data (including locally).
type CloudProvider int

const (
	// CloudProviderNone means not using a cloud provider e.g. using the local filesystem.
	CloudProviderNone CloudProvider = iota

	// CloudProviderAWS means we are using AWS this will likely mean we are accessing data in S3.
	CloudProviderAWS

	// CloudProviderAzure means we are using Azure blob storage.
	CloudProviderAzure

	// CloudProviderGCP means we are using the Google Cloud Platform, this will likely mean we are accessing data stored in
	// Google Storage.
	CloudProviderGCP
)

// String converts the cloud provider into a human readable string representation.
func (c CloudProvider) String() string {
	switch c {
	case CloudProviderNone:
		return ""
	case CloudProviderAWS:
		return "AWS"
	case CloudProviderAzure:
		return "Azure"
	case CloudProviderGCP:
		return "GCP"
	default:
		panic(fmt.Sprintf("unknown cloud provider %d", c))
	}
}

// ToScheme converts CloudProvider to a scheme (e.g. file://).
func (c CloudProvider) ToScheme() string {
	switch c {
	case CloudProviderNone:
		return "file://"
	case CloudProviderAWS:
		return "s3://"
	case CloudProviderAzure:
		return "az://"
	case CloudProviderGCP:
		return "gs://"
	default:
		panic(fmt.Sprintf("unknown cloud provider %d", c))
	}
}
