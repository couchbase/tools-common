package retry

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRetryer(t *testing.T) {
	retryer := NewRetryer[int](RetryerOptions[int]{})

	options := RetryerOptions[int]{
		Algorithm:  AlgorithmFibonacci,
		MaxRetries: 3,
		MinDelay:   50 * time.Millisecond,
		MaxDelay:   2*time.Second + 500*time.Millisecond,
		MinJitter:  50 * time.Millisecond,
		MaxJitter:  250 * time.Millisecond,
	}

	require.Equal(t, Retryer[int]{options: options}, retryer)
}

func TestRetryerDo(t *testing.T) {
	var called int

	payload, err := NewRetryer[struct{}](RetryerOptions[struct{}]{}).Do(func(_ *Context) (struct{}, error) {
		called++
		return struct{}{}, nil
	})

	require.NoError(t, err)
	require.Equal(t, struct{}{}, payload)

	require.Equal(t, 1, called)
}

func TestRetryerDoWithAbort(t *testing.T) {
	var called int

	payload, err := NewRetryer[struct{}](RetryerOptions[struct{}]{}).Do(func(_ *Context) (struct{}, error) {
		called++
		return struct{}{}, NewAbortRetriesError(assert.AnError)
	})

	var aborted *RetriesAbortedError

	require.ErrorAs(t, err, &aborted)
	require.ErrorIs(t, aborted.err, assert.AnError)
	require.Zero(t, payload)
	require.Equal(t, 1, called)
}

func TestRetryerDoWithLogFuncAllButLast(t *testing.T) {
	var (
		called  int
		options = RetryerOptions[int]{
			Log: func(ctx *Context, _ int, _ error) {
				require.NotNil(t, ctx)
				require.Equal(t, called+1, ctx.Attempt())
				called++
			},
		}
	)

	_, err := NewRetryer[int](options).Do(func(_ *Context) (int, error) { return 0, assert.AnError })
	require.Error(t, err)
	require.Equal(t, 2, called)
}

func TestRetryerDoCleanupAllButLast(t *testing.T) {
	var (
		cleanupCalled int
		fnCalled      int
	)

	options := RetryerOptions[int]{
		Cleanup: func(_ int) { cleanupCalled++ },
	}

	payload, err := NewRetryer[int](options).Do(func(_ *Context) (int, error) {
		fnCalled++
		return 0, assert.AnError
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

	payload, err := NewRetryer[int](RetryerOptions[int]{}).Do(func(_ *Context) (int, error) {
		called++
		return 0, assert.AnError
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
		options = RetryerOptions[int]{ShouldRetry: func(_ *Context, _ int, _ error) bool { return false }}
	)

	payload, err := NewRetryer[int](options).Do(
		func(_ *Context) (int, error) { called++; return 0, assert.AnError },
	)

	require.ErrorIs(t, err, assert.AnError)
	require.Zero(t, payload)

	require.Equal(t, 1, called)
}

func TestRetryerDoWithContext(t *testing.T) {
	var called int

	payload, err := NewRetryer[struct{}](RetryerOptions[struct{}]{}).DoWithContext(
		context.Background(),
		func(_ *Context) (struct{}, error) { called++; return struct{}{}, nil },
	)

	require.NoError(t, err)
	require.Equal(t, struct{}{}, payload)

	require.Equal(t, 1, called)
}

func TestRetryerDoWithContextCancelled(t *testing.T) {
	var called int

	fn := func(_ *Context) (int, error) {
		called++

		time.Sleep(100 * time.Millisecond)

		return 0, assert.AnError
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { time.Sleep(50 * time.Millisecond); cancel() }()

	payload, err := NewRetryer[int](RetryerOptions[int]{}).DoWithContext(ctx, fn)

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

	payload, err := NewRetryer[int](RetryerOptions[int]{}).DoWithContext(
		ctx,
		func(_ *Context) (int, error) { called++; return 0, assert.AnError },
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
		algorithm Algorithm
		expected  []time.Duration
	}

	tests := []*test{
		{
			name:      "Fibonacci",
			algorithm: AlgorithmFibonacci,
			expected:  []time.Duration{50 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond},
		},
		{
			name:      "Exponential",
			algorithm: AlgorithmExponential,
			expected:  []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond},
		},
		{
			name:      "Linear",
			algorithm: AlgorithmLinear,
			expected:  []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 150 * time.Millisecond},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for i := 0; i < 3; i++ {
				assert.Equal(
					t,
					test.expected[i],
					NewRetryer[int](RetryerOptions[int]{Algorithm: test.algorithm}).Duration(i+1),
				)
			}
		})
	}
}

func TestRetryerDurationWithMinMax(t *testing.T) {
	type test struct {
		name      string
		algorithm Algorithm
		expected  []time.Duration
	}

	tests := []*test{
		{
			name:      "Fibonacci",
			algorithm: AlgorithmFibonacci,
			expected:  []time.Duration{time.Second, time.Second, 2 * time.Second},
		},
		{
			name:      "Exponential",
			algorithm: AlgorithmExponential,
			expected:  []time.Duration{2 * time.Second, 4 * time.Second, 5 * time.Second},
		},
		{
			name:      "Linear",
			algorithm: AlgorithmLinear,
			expected:  []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for i := 0; i < 3; i++ {
				options := RetryerOptions[int]{
					Algorithm: test.algorithm,
					MinDelay:  time.Second,
					MaxDelay:  5 * time.Second,
				}

				assert.Equal(
					t,
					test.expected[i],
					NewRetryer[int](options).Duration(i+1),
				)
			}
		})
	}
}

func TestRetryerDurationReverse(t *testing.T) {
	type test struct {
		name      string
		algorithm Algorithm
		expected  []time.Duration
	}

	tests := []*test{
		{
			name:      "Fibonacci",
			algorithm: AlgorithmFibonacci,
			expected:  []time.Duration{2500 * time.Millisecond, 1700 * time.Millisecond, 1050 * time.Millisecond},
		},
		{
			name:      "Exponential",
			algorithm: AlgorithmExponential,
			expected:  []time.Duration{2500 * time.Millisecond, 1600 * time.Millisecond, 800 * time.Millisecond},
		},
		{
			name:      "Linear",
			algorithm: AlgorithmLinear,
			expected:  []time.Duration{2500 * time.Millisecond, 2450 * time.Millisecond, 2400 * time.Millisecond},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for i := 0; i < 3; i++ {
				assert.Equal(
					t,
					test.expected[i],
					NewRetryer[int](RetryerOptions[int]{Algorithm: test.algorithm, Reverse: true}).Duration(i+1),
				)
			}
		})
	}
}

func TestRetryerDurationReverseWithMinMax(t *testing.T) {
	type test struct {
		name      string
		algorithm Algorithm
		expected  []time.Duration
	}

	tests := []*test{
		{
			name:      "Fibonacci",
			algorithm: AlgorithmFibonacci,
			expected:  []time.Duration{5 * time.Second, 3 * time.Second, 2 * time.Second},
		},
		{
			name:      "Exponential",
			algorithm: AlgorithmExponential,
			expected:  []time.Duration{5 * time.Second, 4 * time.Second, 2 * time.Second},
		},
		{
			name:      "Linear",
			algorithm: AlgorithmLinear,
			expected:  []time.Duration{5 * time.Second, 4 * time.Second, 3 * time.Second},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for i := 0; i < 3; i++ {
				options := RetryerOptions[int]{
					Algorithm: test.algorithm,
					MinDelay:  time.Second,
					MaxDelay:  5 * time.Second,
					Reverse:   true,
				}

				assert.Equal(
					t,
					test.expected[i],
					NewRetryer[int](options).Duration(i+1),
				)
			}
		})
	}
}

func TestRetryerDurationReverseOverFifty(t *testing.T) {
	const delay = 50 * time.Millisecond

	type test struct {
		name      string
		algorithm Algorithm
		expected  []time.Duration
	}

	tests := []*test{
		{
			name:      "Fibonacci",
			algorithm: AlgorithmFibonacci,
			expected: []time.Duration{
				// fibonacci(50)
				12586269025 * delay,
				// fibonacci(49)
				7778742049 * delay,
				// fibonacci(48)
				4807526976 * delay,
			},
		},
		{
			name:      "Exponential",
			algorithm: AlgorithmExponential,
			expected: []time.Duration{
				// (1 << 38) * delay overflows, so max int is used
				math.MaxInt64,
				// We work backwards from that point
				(1 << 37) * delay,
				(1 << 36) * delay,
			},
		},
		{
			name:      "Linear",
			algorithm: AlgorithmLinear,
			expected: []time.Duration{
				// We don't reach the point of overflow, so we work back from 50
				50 * delay,
				49 * delay,
				48 * delay,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := RetryerOptions[int]{
				Algorithm:  test.algorithm,
				MaxRetries: 100,
				MaxDelay:   math.MaxInt64,
				Reverse:    true,
			}

			// For a reverse retryer, we find the first attempt K that reaches the maximum possible
			// duration. Attempts 1, 2, and 3 should yield the decreasing durations starting from
			// the maximum possible duration.
			for i := 0; i < 3; i++ {
				assert.Equal(
					t,
					test.expected[i],
					NewRetryer[int](options).Duration(i+1),
				)
			}
		})
	}
}

func TestRetryerDurationOverFifty(t *testing.T) {
	const (
		// maxFib is the max fibonacci value that we can use without overflowing
		maxFib = 12586269025 * 50 * time.Millisecond
		// maxExp is 'MaxDelay' as exponential overflows
		maxExp = math.MaxInt64
		// expLin is the expected linear value (as we don't reach anywhere near overflowing)
		expLin = 2*time.Second + 500*time.Millisecond
	)

	type test struct {
		name      string
		algorithm Algorithm
		expected  []time.Duration
	}

	tests := []*test{
		{
			name:      "Fibonacci",
			algorithm: AlgorithmFibonacci,
			expected:  []time.Duration{maxFib, maxFib, maxFib},
		},
		{
			name:      "Exponential",
			algorithm: AlgorithmExponential,
			expected:  []time.Duration{maxExp, maxExp, maxExp},
		},
		{
			name:      "Linear",
			algorithm: AlgorithmLinear,
			expected:  []time.Duration{expLin, expLin, expLin},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := RetryerOptions[int]{
				Algorithm:  test.algorithm,
				MaxRetries: 100,
				MaxDelay:   math.MaxInt64,
			}

			for i := 50; i < 53; i++ {
				assert.Equal(
					t,
					test.expected[i-50],
					NewRetryer[int](options).Duration(i+1),
				)
			}
		})
	}
}

func TestRetryerDurationWithOverflow(t *testing.T) {
	require.Equal(
		t,
		2*time.Second+500*time.Millisecond,
		NewRetryer[int](RetryerOptions[int]{Algorithm: AlgorithmExponential}).Duration(42),
	)
}

func TestRetryerDurationRandomWithDefaults(t *testing.T) {
	options := RetryerOptions[int]{
		Algorithm: AlgorithmRandom,
		MinDelay:  50 * time.Millisecond,
		MaxDelay:  2*time.Second + 500*time.Millisecond,
	}

	retryer := NewRetryer[int](options)

	var last time.Duration

	for i := 0; i < 100; i++ {
		dur := retryer.Duration(i + 1)

		assert.GreaterOrEqual(t, dur, options.MinDelay)
		assert.LessOrEqual(t, dur, options.MaxDelay)
		assert.NotEqual(t, last, dur)

		last = dur
	}
}

func TestRetryerDurationRandomWithEqualMinMax(t *testing.T) {
	retryerEqual := NewRetryer[int](RetryerOptions[int]{
		Algorithm: AlgorithmRandom,
		MinDelay:  time.Second,
		MaxDelay:  time.Second,
	})

	for i := 0; i < 50; i++ {
		assert.Equal(t, time.Second, retryerEqual.Duration(i+1).Round(time.Millisecond))
	}
}
