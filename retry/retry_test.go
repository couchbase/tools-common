package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestWithExponentialRetryAndCancel(t *testing.T) {
	testErr := fmt.Errorf("test")

	t.Run("cancelled-after-500ms", func(t *testing.T) {
		var attempts int
		fn := func() error {
			attempts++
			return testErr
		}

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		err := ExponentialWithContext(ctx, 10, 100*time.Millisecond, fn, nil)
		if !errors.Is(err, ErrIsCancelled) {
			t.Fatalf("Expected cancelled error got %v", err)
		}

		// there is some error here but fn should have been called between 2 and 3 times
		if attempts < 2 || attempts > 4 {
			t.Fatalf("between 2 and 4 retries should have been called instead got: %v", attempts)
		}
	})

	t.Run("success", func(t *testing.T) {
		var attempts int
		fn := func() error {
			attempts++

			if attempts > 3 {
				return nil
			}

			return testErr
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err := ExponentialWithContext(ctx, 10, 50*time.Millisecond, fn, nil)
		if err != nil {
			t.Fatalf("Unexpected error %v", err)
		}
	})

	t.Run("max-retries", func(t *testing.T) {
		var attempts int
		fn := func() error {
			attempts++
			return testErr
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err := ExponentialWithContext(ctx, 5, 50*time.Millisecond, fn, nil)
		if !errors.Is(err, testErr) {
			t.Fatalf("Expected test error got %v", err)
		}

		if attempts != 5 {
			t.Fatalf("Expected 5 retries got %d", attempts)
		}
	})
}
