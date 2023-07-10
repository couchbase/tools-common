package rest

import (
	"context"
	"errors"
	"time"
)

// PollFunc is a readbility wrapper around the function which will be polled by the built in polling functionaility. If
// an error is return from this function, polling will immediately stop and the error will be bubbled up to the calling
// function.
//
// NOTE: This function is called synchronously, therefore, blocking for extended periods of time could cause the
// controlling function to overrun the expected timeout.
type PollFunc func(attempt uint64) (bool, error)

// Poll runs the given polling function until we either reach a timeout, or the polling function returns true. Returns a
// boolean indicating whether we timed out waiting for the polling function to return true.
func (c *Client) Poll(poll func(attempt int) (bool, error)) (bool, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), c.pollTimeout)
	defer cancelFunc()

	return c.PollWithContext(ctx, poll)
}

// PollWithContext runs the given polling function until we either reach a timeout, or the polling function returns
// true. Returns a boolean indicating whether we timed out waiting for the polling function to return true.
//
// NOTE: Returns true in the event that the provided context is cancelled.
func (c *Client) PollWithContext(ctx context.Context, poll func(attempt int) (bool, error)) (bool, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for attempt := 0; ; attempt++ {
		done, err := poll(attempt)
		if err != nil {
			return false, err
		}

		if done || ctx.Err() != nil {
			return errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled), nil
		}

		<-ticker.C
	}
}
