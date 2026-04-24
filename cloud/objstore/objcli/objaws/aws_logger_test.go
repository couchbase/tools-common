package objaws

import (
	"os"
	"testing"

	"github.com/aws/smithy-go/logging"
	"github.com/stretchr/testify/require"
)

func TestNewAWSLogger(t *testing.T) {
	logger := NewAWSLogger()
	require.False(t, logger.enabled)

	// Sanity check the 'Log' function
	logger.Logf(logging.Debug, "log line")
	logger.enabled = true
	logger.Logf(logging.Debug, "log line")
}

func TestNewAWSLoggerForceEnableLogging(t *testing.T) {
	require.NoError(t, os.Setenv("CB_AWS_FORCE_ENABLE_LOGGING", "true"))

	defer func() { require.NoError(t, os.Unsetenv("CB_AWS_FORCE_ENABLE_LOGGING")) }()

	logger := NewAWSLogger()
	require.True(t, logger.enabled)
}
