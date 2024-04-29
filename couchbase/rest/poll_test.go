package rest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPoll(t *testing.T) {
	client := &Client{
		pollTimeout: time.Minute,
	}

	timeout, err := client.Poll(func(_ int) (bool, error) { return true, nil })
	require.NoError(t, err)
	require.False(t, timeout)
}

func TestPollWithContext(t *testing.T) {
	client := &Client{}

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Minute)
	defer cancelFunc()

	timeout, err := client.PollWithContext(ctx, func(_ int) (bool, error) { return true, nil })
	require.NoError(t, err)
	require.False(t, timeout)
}

func TestPollWithContextCancel(t *testing.T) {
	var (
		client          = &Client{}
		ctx, cancelFunc = context.WithTimeout(context.Background(), time.Minute)
	)

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancelFunc()
	}()

	timeout, err := client.PollWithContext(ctx, func(_ int) (bool, error) { return false, nil })
	require.NoError(t, err)
	require.True(t, timeout)
}
