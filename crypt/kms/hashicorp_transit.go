package kms

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/hashicorp/vault/api"
	"gocloud.dev/secrets"
	"gocloud.dev/secrets/hashivault"
)

// getHashiCorpTransitKeeper returns a connection to the HashiCorp Vault transit secret engine that we can use to
// encrypt/decrypt data. The expected format of the URL is [http|https]://<Vault Host>(:<vault port>)?/<key name>
func getHashiCorpTransitKeeper(url, token string) (*secrets.Keeper, error) {
	key, host, err := parseHashiCorpURL(url)
	if err != nil {
		return nil, err
	}

	// Get a client to use with the Vault API.
	client, err := hashivault.Dial(context.Background(), &hashivault.Config{
		Token: token,
		APIConfig: api.Config{
			Address:    host,
			MaxRetries: 3,
			Timeout:    time.Minute,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("could not dial HashiCorp Vault: %w", err)
	}

	return hashivault.OpenKeeper(client, key, nil), nil
}

// parseHashiCorpURL takes a string of the form [http|https]://<host>(:<port>)?/<key name> and separates the key from
// the rest. If the host given is invalid it will fail.
func parseHashiCorpURL(keyURL string) (string, string, error) {
	parsed, err := url.Parse(keyURL)
	if err != nil {
		return "", "", fmt.Errorf("invalid Hashi Corp Vault key url: %w", err)
	}

	if parsed.Host == "" {
		return "", "", fmt.Errorf("a host for the Hashi Corp Vault is required")
	}

	// In the case the path is empty or just "/"
	if len(parsed.Path) <= 1 {
		return "", "", fmt.Errorf("a key name is expected in the Hashi Corp Vault url")
	}

	key := strings.TrimPrefix(parsed.Path, "/")
	parsed.Path = ""

	return key, parsed.String(), nil
}
