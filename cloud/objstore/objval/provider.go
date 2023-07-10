package objval

import "fmt"

// Provider represents a cloud provider.
type Provider int

const (
	// ProviderNone means not using a provider e.g. using the local filesystem.
	ProviderNone Provider = iota

	// ProviderAWS is the AWS cloud provider.
	ProviderAWS

	// ProviderGCP is the Google Cloud Platform cloud provider.
	ProviderGCP

	// ProviderAzure is the Microsoft Azure cloud provider.
	ProviderAzure
)

// String returns a human readable representation of the cloud provider.
func (p Provider) String() string {
	switch p {
	case ProviderNone:
		return ""
	case ProviderAWS:
		return "AWS"
	case ProviderAzure:
		return "Azure"
	case ProviderGCP:
		return "GCP"
	}

	panic(fmt.Sprintf("unknown provider %d", p))
}

// ToScheme converts Provider to a scheme (e.g. file://).
func (p Provider) ToScheme() string {
	switch p {
	case ProviderNone:
		return "file://"
	case ProviderAWS:
		return "s3://"
	case ProviderAzure:
		return "az://"
	case ProviderGCP:
		return "gs://"
	default:
		panic(fmt.Sprintf("unknown provider %d", p))
	}
}
