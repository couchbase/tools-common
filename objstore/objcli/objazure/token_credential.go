package objazure

import (
	"context"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"

	"github.com/couchbase/tools-common/envvar"
	"github.com/couchbase/tools-common/errdefs"
)

// TokenCredential wraps/sets up the 'ChainedTokenCredential' structure for env/imds credentials; this was explicitly
// added to extend the timeout for the IMDS request in the SDK.
type TokenCredential struct {
	chain *azidentity.ChainedTokenCredential
}

// NewTokenCredential returns an initialized 'TokenCredential', will return an error if none of the expected credentials
// are available.
func NewTokenCredential() (*TokenCredential, error) {
	var (
		providers = 2
		creds     = make([]azcore.TokenCredential, 0, providers)
		merr      = &errdefs.MultiError{Prefix: "no token credential providers found: "}
	)

	env, err := azidentity.NewEnvironmentCredential(nil)
	merr.Add(err)

	if env != nil {
		creds = append(creds, env)
	}

	// The IMDS must be last in the chain as the SDK uses an unexported error type to signal that it should continue
	// onto the next item in the chain.
	//
	// See https://github.com/Azure/azure-sdk-for-go/issues/19699#issuecomment-1352295710 for more information.
	mi, err := newWrappedMIC()
	merr.Add(err)

	if mi != nil {
		creds = append(creds, mi)
	}

	// No valid credential providers found, exit early
	if len(merr.Errors()) == providers {
		return nil, merr
	}

	chain, err := azidentity.NewChainedTokenCredential(creds, nil)
	if err != nil {
		return nil, err
	}

	return &TokenCredential{chain: chain}, nil
}

// GetToken implements the SDK 'TokenCredential' interface.
func (t *TokenCredential) GetToken(ctx context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return t.chain.GetToken(ctx, opts)
}

// wrappedMIC implements the 'TokenCredential' interface, wrapping a 'ManagedIdentityCredential' with a 30s timeout for
// the request to the IMDS.
type wrappedMIC struct {
	credential *azidentity.ManagedIdentityCredential
	timeout    time.Duration
}

// newWrappedMIC returns an initialized 'wrappedMIC'.
func newWrappedMIC() (*wrappedMIC, error) {
	options := &azidentity.ManagedIdentityCredentialOptions{
		// Used to indicate to the SDK which managed identity to use, this is explicitly used by 'couchbase-cloud'.
		ID: azidentity.ClientID((os.Getenv("AZURE_CLIENT_ID"))),
	}

	msi, err := azidentity.NewManagedIdentityCredential(options)
	if err != nil {
		return nil, err
	}

	// This timeout has been extended from 1s to 30s, we use an environment variable to give ourselves some breathing
	// room in the future.
	timeout, ok := envvar.GetDuration("CB_OBJAZURE_AZURE_IMDS_TIMEOUT")
	if !ok {
		timeout = 30 * time.Second
	}

	wrapped := wrappedMIC{
		credential: msi,
		timeout:    timeout,
	}

	return &wrapped, nil
}

// GetToken implements the SDK 'TokenCredential' interface.
func (w *wrappedMIC) GetToken(ctx context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	return w.credential.GetToken(ctx, opts)
}
