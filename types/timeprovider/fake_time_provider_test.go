/**
 * Copyright (C) Couchbase, Inc 2025 - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */
package timeprovider

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFakeTimeProviderAdvanceBy(t *testing.T) {
	provider := NewFakeTimeProvider(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	var (
		durs = []time.Duration{5 * time.Millisecond, 25 * time.Millisecond, 125 * time.Millisecond}

		increments atomic.Uint64
		wg         sync.WaitGroup
	)

	for _, dur := range durs {
		wg.Add(1)

		ticker := provider.Ticker()
		ticker.Start(dur)

		go func(ticker Ticker) {
			defer wg.Done()

			for {
				_, ok := <-ticker.Channel()
				if !ok {
					return
				}

				increments.Add(1)
			}
		}(ticker)
	}

	for i := 0; i < 125; i++ {
		provider.AdvanceTimeBy(time.Millisecond)
	}

	for _, ticker := range provider.Tickers {
		close(ticker.ch)
	}

	wg.Wait()

	require.Equal(t, uint64(25+5+1), increments.Load())
}
