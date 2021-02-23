package env

import (
	"os"
	"strconv"
	"time"
)

// GetIntEnvVar returns the int value of the environmental variable varName  if the env var is not an int or empty it
// will return 0, false.
func GetIntEnvVar(varName string) (int, bool) {
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

// GetUintEnvVar returns the uint64 value of the environmental variable varName if the env var is not an int or empty it
// will return 0, false.
func GetUint64EnvVar(varName string) (uint64, bool) {
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

// GetStringEnvVar returns the string value of the environmental variable varName  if the env var is empty it will
// return "", false.
func GetStringEnvVar(varName string) (string, bool) {
	return os.LookupEnv(varName)
}

// GetStringEnvVar returns the boolean value of the environmental variable varName  if the env var is empty or not a
// boolean it will return false, false.
func GetBoolEnvVar(varName string) (bool, bool) {
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

// GetStringEnvVar returns the time.Duration value of the environmental variable varName if the env var is empty or not
// a valid duration string it will return 0, false.
func GetDurationEnvVar(varName string) (time.Duration, bool) {
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
