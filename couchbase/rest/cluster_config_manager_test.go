package rest

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewClusterConfigManager(t *testing.T) {
	manager := NewClusterConfigManager(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	require.NotZero(t, manager.last)
	require.Equal(t, 15*time.Second, manager.maxAge)
	require.NotZero(t, manager.cond)
	require.NotZero(t, manager.signal)
}

func TestNewClusterConfigManagerWithMaxAge(t *testing.T) {
	os.Setenv("CB_REST_CC_MAX_AGE", "1m")
	defer os.Unsetenv("CB_REST_CC_MAX_AGE")

	manager := NewClusterConfigManager(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	require.NotZero(t, manager.last)
	require.Equal(t, time.Minute, manager.maxAge)
	require.NotZero(t, manager.cond)
	require.NotZero(t, manager.signal)
}

func TestClusterConfigManagerUpdate(t *testing.T) {
	type test struct {
		name             string
		current, updated *ClusterConfig
		old              bool
	}

	tests := []*test{
		{
			name:    "CurrentIsNil",
			updated: &ClusterConfig{Revision: 42},
		},
		{
			name:    "NewIsLesserRev",
			current: &ClusterConfig{Revision: 64},
			updated: &ClusterConfig{Revision: 42},
			old:     true,
		},
		{
			name:    "NewIsEqualRev",
			current: &ClusterConfig{Revision: 42},
			updated: &ClusterConfig{Revision: 42},
		},
		{
			name:    "NewIsGreaterRev",
			current: &ClusterConfig{Revision: 42},
			updated: &ClusterConfig{Revision: 64},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				woken   bool
				manager = NewClusterConfigManager(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
			)

			manager.config = test.current

			go func() {
				manager.cond.L.Lock()
				defer manager.cond.L.Unlock()

				manager.cond.Wait()

				woken = true
			}()

			time.Sleep(50 * time.Millisecond)

			last := manager.last

			err := manager.Update(test.updated)

			if test.old {
				var oldClusterConfig *OldClusterConfigError

				require.ErrorAs(t, err, &oldClusterConfig)
				require.NotSame(t, test.updated, manager.config)
				require.Equal(t, last, manager.last)

				return
			}

			time.Sleep(50 * time.Millisecond)

			require.NoError(t, err)
			require.NotEqual(t, last, manager.last)
			require.Greater(t, manager.last.UTC().UnixNano(), last.UTC().UnixNano())
			require.Same(t, test.updated, manager.config)
			require.True(t, woken)
		})
	}
}

func TestClusterConfigManagerWaitUntilUpdated(t *testing.T) {
	var (
		woken   bool
		manager = NewClusterConfigManager(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	)

	go func() {
		manager.WaitUntilUpdated(context.Background())

		woken = true
	}()

	time.Sleep(50 * time.Millisecond)

	err := manager.Update(&ClusterConfig{Revision: 42})

	time.Sleep(50 * time.Millisecond)

	require.NoError(t, err)
	require.True(t, woken)
}

func TestClusterConfigManagerWaitUntilUpdatedContextCancel(t *testing.T) {
	var (
		woken       bool
		ctx, cancel = context.WithCancel(context.Background())
		manager     = NewClusterConfigManager(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	)

	go func() {
		manager.WaitUntilUpdated(ctx)

		woken = true
	}()

	time.Sleep(50 * time.Millisecond)

	cancel()

	time.Sleep(50 * time.Millisecond)

	require.True(t, woken)
}

func TestClusterConfigManagerWaitUntilUpdatedSmokeTest(t *testing.T) {
	var (
		woken   = make([]bool, 1024)
		manager = NewClusterConfigManager(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	)

	for i := 0; i < 1024; i++ {
		go func(i int) {
			manager.WaitUntilUpdated(context.Background())

			woken[i] = true
		}(i)
	}

	time.Sleep(50 * time.Millisecond)

	err := manager.Update(&ClusterConfig{Revision: 42})

	time.Sleep(50 * time.Millisecond)

	require.NoError(t, err)

	for i := 0; i < 1024; i++ {
		require.True(t, woken[i])
	}
}

func TestClusterConfigManagerWaitUntilExpired(t *testing.T) {
	os.Setenv("CB_REST_CC_MAX_AGE", "50ms")
	defer os.Unsetenv("CB_REST_CC_MAX_AGE")

	var (
		woken   bool
		manager = NewClusterConfigManager(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	)

	go func() {
		manager.WaitUntilExpired(context.Background())

		woken = true
	}()

	// Sleep for less than the max age to avoid racing for the condition below
	time.Sleep(25 * time.Millisecond)

	require.False(t, woken)

	time.Sleep(100 * time.Millisecond)

	require.True(t, woken)
}

func TestClusterConfigManagerWaitUntilExpiredContextCancel(t *testing.T) {
	var (
		woken       bool
		ctx, cancel = context.WithCancel(context.Background())
		manager     = NewClusterConfigManager(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	)

	go func() {
		manager.WaitUntilExpired(ctx)

		woken = true
	}()

	time.Sleep(50 * time.Millisecond)

	cancel()

	time.Sleep(50 * time.Millisecond)

	require.True(t, woken)
}
