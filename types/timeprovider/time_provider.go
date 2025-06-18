package timeprovider

import "time"

//go:generate mockery --all --case underscore --inpackage

type TimeProvider interface {
	Now() time.Time
}

type CurrentTimeProvider struct{}

func (tp CurrentTimeProvider) Now() time.Time {
	return time.Now()
}
