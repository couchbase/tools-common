package timeprovider

import "time"

//go:generate mockery --all --case underscore --inpackage

type TimeProvider interface {
	Now() time.Time
	Ticker() Ticker
}

type CurrentTimeProvider struct{}

var (
	_ TimeProvider = (*CurrentTimeProvider)(nil)
	_ TimeProvider = (*MockTimeProvider)(nil)
)

func (tp CurrentTimeProvider) Now() time.Time {
	return time.Now()
}

func (tp CurrentTimeProvider) Ticker() Ticker {
	return NewRealTicker()
}
