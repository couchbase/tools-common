/**
 * Copyright (C) Couchbase, Inc 2025 - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */

package timeprovider

import "time"

// Ticker is an interface for a timer that can be started, stopped, and queried for ticks, like 'time.Ticker'. We
// define this interface so that we can use a fake ticker in tests.
type Ticker interface {
	Start(duration time.Duration)
	Channel() <-chan time.Time
	Stop()
}

var (
	_ Ticker = (*RealTicker)(nil)
	_ Ticker = (*FakeTicker)(nil)
	_ Ticker = (*MockTicker)(nil)
)

// RealTicker is a wrapper around 'time.Ticker' that implements the 'Ticker' interface.
type RealTicker struct {
	ticker *time.Ticker
}

func NewRealTicker() *RealTicker {
	return &RealTicker{}
}

func (r *RealTicker) Start(duration time.Duration) {
	r.ticker = time.NewTicker(duration)
}

func (r *RealTicker) Channel() <-chan time.Time {
	return r.ticker.C
}

func (r *RealTicker) Stop() {
	r.ticker.Stop()
}

// FakeTicker is a fake implementation of the 'Ticker' interface that can be used in tests. Whenever we want to run an
// interaction 'ForceTick' or 'TickIfElapsed' can be called.
type FakeTicker struct {
	provider TimeProvider
	ch       chan time.Time
	dur      time.Duration
	lastTick time.Time
}

func NewFakeTicker(provider TimeProvider) *FakeTicker {
	return &FakeTicker{ch: make(chan time.Time), provider: provider, lastTick: provider.Now()}
}

// ForceTick sends the current time to the channel.
func (f *FakeTicker) ForceTick() {
	now := f.provider.Now()
	f.ch <- now
	f.lastTick = now
}

// TickIfElapsed sends the current time to the channel if the timer has elapsed.
func (f *FakeTicker) TickIfElapsed() {
	if f.dur == 0 {
		return
	}

	var (
		nextTick = f.lastTick.Add(f.dur)
		now      = f.provider.Now()
	)

	if nextTick.Before(now) || nextTick == now {
		f.ForceTick()
	}
}

// Start begins the ticker. As this is a fake, we just record 'dur' and the current time, allowing us to check whether
// we have elapsed.
func (f *FakeTicker) Start(dur time.Duration) {
	f.lastTick = f.provider.Now()
	f.dur = dur
}

func (f *FakeTicker) Channel() <-chan time.Time {
	return f.ch
}

func (f *FakeTicker) Stop() {
	f.dur = 0
}
