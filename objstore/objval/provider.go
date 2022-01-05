package objval

import "fmt"

// Provider represents a cloud provider.
type Provider int

const (
	// ProviderAWS is the AWS cloud provider.
	ProviderAWS Provider = iota + 1

	// ProviderGCP is the Google Cloud Platform cloud provider.
	ProviderGCP

	// ProviderAzure is the Microsoft Azure cloud provider.
	ProviderAzure
)

// String returns a human readable representation of the cloud provider.
func (p Provider) String() string {
	switch p {
	case ProviderAWS:
		return "AWS"
	case ProviderAzure:
		return "Azure"
	case ProviderGCP:
		return "GCP"
	}

	panic(fmt.Sprintf("unknown provider %d", p))
}
