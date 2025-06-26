/**
 * Copyright (C) Couchbase, Inc 2025 - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */
package timeprovider

import (
	"time"
)

// FakeTimeProvider implements 'TimeProvider' and allows the time to be advanced. When this happens and a ticker has
// elapsed it is ticked.
type FakeTimeProvider struct {
	Time    time.Time
	Tickers []*FakeTicker
}

var _ TimeProvider = &FakeTimeProvider{}

func NewFakeTimeProvider(start time.Time) *FakeTimeProvider {
	return &FakeTimeProvider{Time: start, Tickers: make([]*FakeTicker, 0)}
}

func (f *FakeTimeProvider) Now() time.Time {
	return f.Time
}

func (f *FakeTimeProvider) Ticker() Ticker {
	ticker := NewFakeTicker(f)
	f.Tickers = append(f.Tickers, ticker)

	return ticker
}

// AdvanceTimeTo sets the time to 't', ticking any tickers that have elapsed.
func (f *FakeTimeProvider) AdvanceTimeTo(t time.Time) {
	f.Time = t
	for _, ticker := range f.Tickers {
		ticker.TickIfElapsed()
	}
}

// AdvanceTimeBy advances the time by 'd', ticking any tickers that have elapsed.
func (f *FakeTimeProvider) AdvanceTimeBy(d time.Duration) {
	f.AdvanceTimeTo(f.Time.Add(d))
}
