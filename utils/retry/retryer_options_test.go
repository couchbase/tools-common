package retry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryerOptionsDefaults(t *testing.T) {
	options := RetryerOptions[int]{}
	options.defaults()

	expected := RetryerOptions[int]{
		Algorithm:  AlgorithmFibonacci,
		MaxRetries: 3,
		MinDelay:   50 * time.Millisecond,
		MaxDelay:   2*time.Second + 500*time.Millisecond,
		MinJitter:  50 * time.Millisecond,
		MaxJitter:  250 * time.Millisecond,
	}

	require.Equal(t, expected, options)
}
