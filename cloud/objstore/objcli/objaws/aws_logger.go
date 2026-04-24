package objaws

import (
	"log"

	"github.com/aws/smithy-go/logging"

	envvar "github.com/couchbase/tools-common/environment/variable"
)

const s3LogPrefix = "(AWS)"

// AWSLogger - Implementation of the logger interface for AWS which redirects logging using 'log'.
type AWSLogger struct {
	enabled bool
}

var _ logging.Logger = (*AWSLogger)(nil)

// NewAWSLogger - Create a new AWS logger which by default is disabled meaning all logging is ignored.
func NewAWSLogger() *AWSLogger {
	enabled, ok := envvar.GetBool("CB_AWS_FORCE_ENABLE_LOGGING")
	if ok && enabled {
		log.Printf("%s Force enabled logging\n", s3LogPrefix)
	}

	return &AWSLogger{
		enabled: enabled,
	}
}

// SetEnabled - Set logging to enabled/disabled.
func (a *AWSLogger) SetEnabled(enabled bool) {
	a.enabled = enabled
}

// GetEnabled - Get whether logging is enabled/disabled.
func (a *AWSLogger) GetEnabled() bool {
	return a.enabled
}

// Log - Implement the logger interface for the AWS SDK; this function simply redirects logging using 'log'.
func (a *AWSLogger) Logf(_ logging.Classification, format string, v ...any) {
	// Since we have to do a fair amount of "stuff" before logging is setup, logging will be disabled by default until
	// we have mounted the archive and setup the log file.
	if !a.enabled {
		return
	}

	log.Printf(format, v...)
}
