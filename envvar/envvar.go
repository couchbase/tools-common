package envvar

import (
	"os"
	"strconv"
	"time"
)

// GetInt returns the int value of the environmental variable varName  if the env var is not an int or empty it will
// return 0, false.
func GetInt(varName string) (int, bool) {
	env, ok := os.LookupEnv(varName)
	if !ok {
		return 0, false
	}

	val, err := strconv.Atoi(env)
	if err != nil {
		return 0, false
	}

	return val, true
}

// GetUint64 returns the uint64 value of the environmental variable varName if the env var is not an int or empty it
// will return 0, false.
func GetUint64(varName string) (uint64, bool) {
	env, ok := os.LookupEnv(varName)
	if !ok {
		return 0, false
	}

	val, err := strconv.ParseUint(env, 10, 64)
	if err != nil {
		return 0, false
	}

	return val, true
}

// GetBool returns the boolean value of the environmental variable varName  if the env var is empty or not a boolean it
// will return false, false.
func GetBool(varName string) (bool, bool) {
	val, ok := os.LookupEnv(varName)
	if !ok {
		return false, false
	}

	ret, err := strconv.ParseBool(val)
	if err != nil {
		return false, false
	}

	return ret, true
}

// GetDuration returns the time.Duration value of the environmental variable varName if the env var is empty or not a
// valid duration string it will return 0, false.
func GetDuration(varName string) (time.Duration, bool) {
	val, ok := os.LookupEnv(varName)
	if !ok {
		return 0, false
	}

	duration, err := time.ParseDuration(val)
	if err != nil {
		return 0, false
	}

	return duration, true
}

// GetDurationBC returns the time.Duration value of the environmental variable varName if the env var is empty or not a
// valid duration string it will return 0, false.
//
// NOTE: This is the backwards compatible variant of 'GetDuration' meaning when failing to parse a duration, it will
// fallback to parsing an integer number of seconds.
func GetDurationBC(varName string) (time.Duration, bool) {
	duration, ok := GetDuration(varName)
	if ok {
		return duration, true
	}

	raw, ok := GetInt(varName)
	if ok {
		return time.Duration(raw) * time.Second, true
	}

	return 0, false
}
