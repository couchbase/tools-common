package retry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRetryer(t *testing.T) {
	retryer := NewRetryer(RetryerOptions{})

	options := RetryerOptions{
		Algoritmn:  AlgoritmnFibonacci,
		MaxRetries: 3,
		MinDelay:   50 * time.Millisecond,
		MaxDelay:   2*time.Second + 500*time.Millisecond,
	}

	require.Equal(t, Retryer{options: options}, retryer)
}

func TestRetryerDo(t *testing.T) {
	var called int

	payload, err := NewRetryer(RetryerOptions{}).Do(func(ctx *Context) (interface{}, error) {
		called++
		return struct{}{}, nil
	})

	require.NoError(t, err)
	require.Equal(t, struct{}{}, payload)

	require.Equal(t, 1, called)
}

func TestRetryerDoWithLogFuncAllButLast(t *testing.T) {
	var (
		called  int
		options = RetryerOptions{
			Log: func(ctx *Context, _ interface{}, err error) {
				require.NotNil(t, ctx)
				require.Equal(t, called+1, ctx.Attempt())
				called++
			},
		}
	)

	_, err := NewRetryer(options).Do(func(ctx *Context) (interface{}, error) { return nil, assert.AnError })
	require.Error(t, err)
	require.Equal(t, 2, called)
}

func TestRetryerDoCleanupAllButLast(t *testing.T) {
	var (
		cleanupCalled int
		fnCalled      int
	)

	options := RetryerOptions{
		Cleanup: func(payload interface{}) { cleanupCalled++ },
	}

	payload, err := NewRetryer(options).Do(func(ctx *Context) (interface{}, error) {
		fnCalled++
		return nil, assert.AnError
	})

	var retriesExhausted *RetriesExhaustedError

	require.ErrorAs(t, err, &retriesExhausted)
	require.ErrorIs(t, err, assert.AnError)
	require.Zero(t, payload)

	require.Equal(t, 2, cleanupCalled)
	require.Equal(t, 3, fnCalled)
}

func TestRetryerDoWithError(t *testing.T) {
	var called int

	payload, err := NewRetryer(RetryerOptions{}).Do(func(ctx *Context) (interface{}, error) {
		called++
		return nil, assert.AnError
	})

	var retriesExhausted *RetriesExhaustedError

	require.ErrorAs(t, err, &retriesExhausted)
	require.ErrorIs(t, err, assert.AnError)
	require.Zero(t, payload)

	require.Equal(t, 3, called)
}

func TestRetryerDoShouldNotRetry(t *testing.T) {
	var (
		called  int
		options = RetryerOptions{ShouldRetry: func(_ *Context, _ interface{}, _ error) bool { return false }}
	)

	payload, err := NewRetryer(options).Do(
		func(ctx *Context) (interface{}, error) { called++; return nil, assert.AnError },
	)

	require.ErrorIs(t, err, assert.AnError)
	require.Zero(t, payload)

	require.Equal(t, 1, called)
}

func TestRetryerDoWithContext(t *testing.T) {
	var called int

	payload, err := NewRetryer(RetryerOptions{}).DoWithContext(
		context.Background(),
		func(ctx *Context) (interface{}, error) { called++; return struct{}{}, nil },
	)

	require.NoError(t, err)
	require.Equal(t, struct{}{}, payload)

	require.Equal(t, 1, called)
}

func TestRetryerDoWithContextCancelled(t *testing.T) {
	var called int

	fn := func(ctx *Context) (interface{}, error) {
		called++

		time.Sleep(100 * time.Millisecond)

		return nil, assert.AnError
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { time.Sleep(50 * time.Millisecond); cancel() }()

	payload, err := NewRetryer(RetryerOptions{}).DoWithContext(ctx, fn)

	var retriesAborted *RetriesAbortedError

	require.ErrorAs(t, err, &retriesAborted)
	require.Equal(t, 1, retriesAborted.attempts)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, payload)

	require.Equal(t, 1, called)
}

func TestRetryerInternalDoContextCancelled(t *testing.T) {
	var called int

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	payload, err := NewRetryer(RetryerOptions{}).DoWithContext(
		ctx,
		func(ctx *Context) (interface{}, error) { called++; return nil, assert.AnError },
	)

	var retriesAborted *RetriesAbortedError

	require.ErrorAs(t, err, &retriesAborted)
	require.Zero(t, retriesAborted.attempts)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, payload)

	require.Zero(t, called)
}

func TestRetryerDuration(t *testing.T) {
	type test struct {
		name      string
		algorithm Algoritmn
		expected  []time.Duration
	}

	tests := []*test{
		{
			name:      "Fibonacci",
			algorithm: AlgoritmnFibonacci,
			expected:  []time.Duration{50 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond},
		},
		{
			name:      "Exponential",
			algorithm: AlgoritmnExponential,
			expected:  []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond},
		},
		{
			name:      "Linear",
			algorithm: AlgoritmnLinear,
			expected:  []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 150 * time.Millisecond},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for i := 0; i < 3; i++ {
				require.Equal(
					t,
					test.expected[i],
					NewRetryer(RetryerOptions{Algoritmn: test.algorithm}).duration(i+1),
				)
			}
		})
	}
}

func TestRetryerDurationWithOverflow(t *testing.T) {
	require.Equal(
		t,
		2*time.Second+500*time.Millisecond,
		NewRetryer(RetryerOptions{Algoritmn: AlgoritmnExponential}).duration(42),
	)
}
