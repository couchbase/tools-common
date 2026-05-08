package kms

import (
	"context"
	"fmt"

	cloudkms "cloud.google.com/go/kms/apiv1"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/cloud/kms/v1" //nolint:staticcheck
)

type gcpKeeper struct {
	keyID  string
	client *cloudkms.KeyManagementClient
}

func (k *gcpKeeper) Close() error {
	return k.client.Close()
}

func (k *gcpKeeper) Encrypt(ctx context.Context, plainText []byte) ([]byte, error) {
	res, err := k.client.Encrypt(ctx, &kms.EncryptRequest{Plaintext: plainText, Name: k.keyID}) //nolint:staticcheck
	if err != nil {
		return nil, fmt.Errorf("could not encrypt data: %w", err)
	}

	return res.GetCiphertext(), nil
}

func (k *gcpKeeper) Decrypt(ctx context.Context, cipherText []byte) ([]byte, error) {
	res, err := k.client.Decrypt(ctx, &kms.DecryptRequest{Ciphertext: cipherText, Name: k.keyID}) //nolint:staticcheck
	if err != nil {
		return nil, fmt.Errorf("could not decrypt data: %w", err)
	}

	return res.GetPlaintext(), nil
}

// getGCPKeeper dials the GCP KMS and returns a client to it. The available auth options are:
// 1. Service account via the GOOGLE_APPLICATION_CREDENTIALS environmental variable.
// 2. Service account JSON explicitly passed via serviceJSON.
// 3. Service account file explicitly passed via pathToServiceFile.
// 4. In GCP the client will find credentials by itself.
func getGCPKeeper(url, pathToServiceFile string, serviceJSON []byte) (Keeper, error) {
	var clientOpts option.ClientOption

	// These functions are deprecated but we still need them, so ignore the lint. See MB-71354
	if serviceJSON != nil {
		clientOpts = option.WithCredentialsJSON(serviceJSON) //nolint:staticcheck
	} else if pathToServiceFile != "" {
		clientOpts = option.WithCredentialsFile(pathToServiceFile) //nolint:staticcheck
	}

	keeper := &gcpKeeper{keyID: url}

	var err error
	if clientOpts == nil {
		keeper.client, err = cloudkms.NewKeyManagementClient(context.Background())
	} else {
		keeper.client, err = cloudkms.NewKeyManagementClient(context.Background(), clientOpts)
	}

	if err != nil {
		return nil, fmt.Errorf("could not get GCP kms client: %w", err)
	}

	return keeper, nil
}
