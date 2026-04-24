package kms

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/kms"
	"gocloud.dev/secrets"
	"gocloud.dev/secrets/awskms"

	"github.com/couchbase/tools-common/cloud/v8/objstore/objcli/objaws"
	"github.com/couchbase/tools-common/cloud/v8/objstore/objval"
)

// getAWSKeeper creates a custom aws session and uses it to dial aws KMS.
func getAWSKeeper(
	url, region, secretAccessKey, accessKeyID, refreshToken, overrideEndpoint string,
) (*secrets.Keeper, error) {
	config, _, err := objaws.AWSNewConfig(context.Background(), objaws.AWSOptions{
		Region:          region,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		RefreshToken:    refreshToken,
	})
	if err != nil {
		return nil, fmt.Errorf("could not create aws session options: %w", err)
	}

	endpoint := objaws.AddSchemeIfMissing(overrideEndpoint, objval.ProviderAWS)

	// Get a client to use with the KMS API.
	client := kms.NewFromConfig(config, func(o *kms.Options) {
		if endpoint != "" {
			o.BaseEndpoint = &endpoint
		}
	})

	// Construct a *secrets.Keeper.
	return awskms.OpenKeeperV2(client, url, nil), nil
}
