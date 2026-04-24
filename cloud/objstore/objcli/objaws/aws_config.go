package objaws

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"

	"github.com/couchbase/tools-common/cloud/v8/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v8/objstore/objval"
	envvar "github.com/couchbase/tools-common/environment/variable"
)

// AWSOptions contains the options used by AWSNewConfig. It mirrors the fields
// of CloudContainerOptions that are used when building an AWS config.
type AWSOptions struct {
	TLSConfig              *tls.Config
	Region                 string
	AccessKeyID            string
	SecretAccessKey        string
	RefreshToken           string
	AuthByInstanceMetaData bool
	LogLevel               string
}

var (
	// ErrStaticCredentialsRequired - Returned if the user has disabled shared config loading but they haven't provided
	// static credentials.
	ErrStaticCredentialsRequired = errors.New("when shared config and instance metadata " +
		"are both disabled, static credentials must be provided")

	// ErrRegionRequiredWhenStaticConfigDisabled - Returned if the user has disabled shared config loading but they
	// haven't provided the '--obj-region' flag.
	ErrRegionRequiredWhenStaticConfigDisabled = errors.New("when shared config is disabled, a region must be provided")
)

func AWSNewConfig(ctx context.Context, options AWSOptions) (aws.Config, *AWSLogger, error) {
	client, err := objcli.NewHTTPClient(options.TLSConfig, nil)
	if err != nil {
		return aws.Config{}, nil, fmt.Errorf("failed to get HTTP client: %w", err)
	}

	logger := NewAWSLogger()

	configs := make([]func(*config.LoadOptions) error, 0)
	configs = append(configs,
		config.WithHTTPClient(client),
		config.WithRetryer(func() aws.Retryer { return NewAWSRetryer() }),
		config.WithLogger(logger),
	)

	credentialExpiryWindow, ok := envvar.GetDuration("CB_AWS_CREDENTIALS_EXPIRY_WINDOW")
	if !ok {
		credentialExpiryWindow = 5 * time.Second
	}

	configs = append(configs, config.WithCredentialsCacheOptions(func(o *aws.CredentialsCacheOptions) {
		o.ExpiryWindow = credentialExpiryWindow
	}))

	if options.Region != "" {
		configs = append(configs, config.WithRegion(options.Region))
	}

	var provider aws.CredentialsProvider

	if options.AccessKeyID != "" && options.SecretAccessKey != "" {
		provider = aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider(options.AccessKeyID, options.SecretAccessKey, options.RefreshToken))
	}

	imdsState := imds.ClientDisabled

	// Only enable getting credentials from IMDS if it is asked for. When provided with insufficient credentials and
	// not running in an EC2 instance the http client timeout/retries can make it seem like we are hanging when trying
	// to fetch EC2 metadata.
	enabled, ok := envvar.GetBool("CB_AWS_ENABLE_EC2_METADATA")
	if options.AuthByInstanceMetaData || (ok && enabled) {
		imdsState = imds.ClientEnabled
	}

	configs = append(configs, config.WithEC2IMDSClientEnableState(imdsState))

	var (
		logMode  aws.ClientLogMode
		logDebug = aws.LogRequest | aws.LogResponse
	)

	switch options.LogLevel {
	case "":
		// Do nothing, the user hasn't specified a log level
	case "debug-with-event-stream-body":
		logMode = aws.LogRequestEventMessage | logDebug
	case "debug-with-request-retries":
		logMode = aws.LogRetries | logDebug
	case "debug-with-body":
		logMode = aws.LogRequestWithBody | logDebug
	case "debug-with-signing":
		logMode = aws.LogSigning | logDebug
	case "debug":
		logMode = logDebug
	default:
		return aws.Config{}, nil, fmt.Errorf("invalid log level '%s' expected one of ['debug', "+
			"'debug-with-signing', 'debug-with-body', 'debug-with-request-retries', "+
			"'debug-with-event-stream-body']", options.LogLevel)
	}

	configs = append(configs, config.WithClientLogMode(logMode))

	// If the user is specifically turning off shared config loading then we need to do some extra validation to ensure
	// they have passed enough information to actually authenticate.
	if loadConfig, ok := envvar.GetBool("AWS_SDK_LOAD_CONFIG"); ok && !loadConfig {
		// If the user hasn't provided any credentials and is also disabling the shared config functionality then
		// we should fail fast since we have no valid credentials.
		if !options.AuthByInstanceMetaData && provider == nil {
			return aws.Config{}, nil, ErrStaticCredentialsRequired
		}

		// If we have not been provided with a region whilst disabling the shared config functionality we should also
		// fail fast since this will be picked up by the AWS SDK.
		if options.Region == "" {
			return aws.Config{}, nil, ErrRegionRequiredWhenStaticConfigDisabled
		}
	}

	if provider != nil {
		configs = append(configs, config.WithCredentialsProvider(provider))
	}

	cfg, err := config.LoadDefaultConfig(ctx, configs...)
	if err != nil {
		return aws.Config{}, nil, fmt.Errorf("failed to load default config: %w", err)
	}

	return cfg, logger, nil
}

var schemeRE = regexp.MustCompile("^([^:]+)://")

// AddSchemeIfMissing - Returns the endpoint with "https://" prepended if it doesn't already have a scheme.
// If the endpoint is empty, a scheme is present, or the provider is not AWS, the endpoint is returned as is.
func AddSchemeIfMissing(endpoint string, provider objval.Provider) string {
	if endpoint == "" || provider != objval.ProviderAWS {
		return endpoint
	}

	// If the endpoint already has a scheme then we don't need to add one.
	if schemeRE.MatchString(endpoint) {
		return endpoint
	}

	return "https://" + endpoint
}
