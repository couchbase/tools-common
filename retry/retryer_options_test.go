package retry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryerOptionsDefaults(t *testing.T) {
	options := RetryerOptions{}
	options.defaults()

	expected := RetryerOptions{
		Algoritmn:  AlgoritmnFibonacci,
		MaxRetries: 3,
		MinDelay:   50 * time.Millisecond,
		MaxDelay:   2*time.Second + 500*time.Millisecond,
	}

	require.Equal(t, expected, options)
}

func TestRetryerOptionsLimitRetries(t *testing.T) {
	options := RetryerOptions{MaxRetries: 51}
	options.defaults()

	expected := RetryerOptions{
		Algoritmn:  AlgoritmnFibonacci,
		MaxRetries: 50,
		MinDelay:   50 * time.Millisecond,
		MaxDelay:   2*time.Second + 500*time.Millisecond,
	}

	require.Equal(t, expected, options)
}
