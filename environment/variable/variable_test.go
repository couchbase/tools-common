package variable

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetInt(t *testing.T) {
	type test struct {
		name         string
		expectedVal  int
		expectedBool bool
		envName      string
		envValue     string
	}

	tests := []test{
		{
			name:         "ValidEnv",
			expectedVal:  10,
			expectedBool: true,
			envName:      "CB_TEST_ENVAR_VALID_CASE",
			envValue:     "10",
		},
		{
			name:    "EnvNotSet",
			envName: "CB_TEST_ENVAR_NO_ENV_CASE",
		},
		{
			name:     "EnvIsNotAnInt",
			envName:  "CB_TEST_ENVAR_NOT_AN_INT_CASE",
			envValue: "this is not an int",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, os.Setenv(test.envName, test.envValue))
			defer os.Unsetenv(test.envName)

			val, ok := GetInt(test.envName)

			require.Equal(t, test.expectedBool, ok)
			require.Equal(t, test.expectedVal, val)
		})
	}
}

func TestGetUint64(t *testing.T) {
	type test struct {
		name         string
		expectedVal  uint64
		expectedBool bool
		envName      string
		envValue     string
	}

	tests := []test{
		{
			name:         "ValidEnv",
			expectedVal:  10,
			expectedBool: true,
			envName:      "CB_TEST_ENVAR_VALID_CASE",
			envValue:     "10",
		},
		{
			name:    "EnvNotSet",
			envName: "CB_TEST_ENVAR_NO_ENV_CASE",
		},
		{
			name:     "EnvIsNotAUint64",
			envName:  "CB_TEST_ENVAR_NOT_AN_INT_CASE",
			envValue: "this is not an int",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, os.Setenv(test.envName, test.envValue))
			defer os.Unsetenv(test.envName)

			val, ok := GetUint64(test.envName)

			require.Equal(t, test.expectedBool, ok)
			require.Equal(t, test.expectedVal, val)
		})
	}
}

func TestGetBool(t *testing.T) {
	type test struct {
		name          string
		value         string
		envName       string
		expectedBool  bool
		expectedValue bool
	}

	tests := []test{
		{
			name:    "NotSet",
			envName: "CB_TEST_ABCDEFG",
		},
		{
			name:          "SetToTrue",
			value:         "true",
			envName:       "CB_TEST_ABCDEFG",
			expectedBool:  true,
			expectedValue: true,
		},
		{
			name:         "SetToFalse",
			value:        "false",
			envName:      "CB_TEST_ABCDEFG",
			expectedBool: true,
		},
		{
			name:          "SetToTruey",
			value:         "t",
			envName:       "CB_TEST_ABCDEFG",
			expectedBool:  true,
			expectedValue: true,
		},
		{
			name:         "SetToFalsy",
			value:        "f",
			envName:      "CB_TEST_ABCDEFG",
			expectedBool: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, os.Setenv(test.envName, test.value))
			defer os.Unsetenv(test.envName)

			out, ok := GetBool(test.envName)

			require.Equal(t, test.expectedBool, ok)
			require.Equal(t, test.expectedValue, out)
		})
	}
}

func TestGetDuration(t *testing.T) {
	type test struct {
		name             string
		value            string
		envName          string
		expectedBool     bool
		expectedDuration time.Duration
	}

	tests := []test{
		{
			name:             "SetToSeconds",
			value:            "1s",
			envName:          "CB_TEST_ABCDEFG",
			expectedBool:     true,
			expectedDuration: time.Second,
		},
		{
			name:             "SetToSecondsAndMinutes",
			value:            "33m1s",
			envName:          "CB_TEST_ABCDEFG",
			expectedBool:     true,
			expectedDuration: 33*time.Minute + 1*time.Second,
		},
		{
			name:    "UnsetEnv",
			value:   "",
			envName: "CB_TEST_ABCDEFG_amdkasd",
		},
		{
			name:    "InvalidTimeString",
			value:   "7.87.232.498",
			envName: "CB_TEST_ABCDEFG",
		},
		{
			name:    "NotADurationString",
			value:   "60",
			envName: "CB_TEST_ABCDEFG",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, os.Setenv(test.envName, test.value))
			defer os.Unsetenv(test.envName)

			out, ok := GetDuration(test.envName)

			require.Equal(t, test.expectedBool, ok)
			require.Equal(t, test.expectedDuration, out)
		})
	}
}

func TestGetDurationBC(t *testing.T) {
	type test struct {
		name             string
		value            string
		envName          string
		expectedBool     bool
		expectedDuration time.Duration
	}

	tests := []test{
		{
			name:             "SetToSeconds",
			value:            "1s",
			envName:          "CB_TEST_ABCDEFG",
			expectedBool:     true,
			expectedDuration: time.Second,
		},
		{
			name:             "SetToSecondsAndMinutes",
			value:            "33m1s",
			envName:          "CB_TEST_ABCDEFG",
			expectedBool:     true,
			expectedDuration: 33*time.Minute + 1*time.Second,
		},
		{
			name:    "UnsetEnv",
			value:   "",
			envName: "CB_TEST_ABCDEFG_amdkasd",
		},
		{
			name:    "InvalidTimeString",
			value:   "7.87.232.498",
			envName: "CB_TEST_ABCDEFG",
		},
		{
			name:             "NotADurationString",
			value:            "60",
			envName:          "CB_TEST_ABCDEFG",
			expectedBool:     true,
			expectedDuration: 60 * time.Second,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, os.Setenv(test.envName, test.value))
			defer os.Unsetenv(test.envName)

			out, ok := GetDurationBC(test.envName)

			require.Equal(t, test.expectedBool, ok)
			require.Equal(t, test.expectedDuration, out)
		})
	}
}

func TestGetBytes(t *testing.T) {
	type test struct {
		name          string
		value         string
		envName       string
		expectedBool  bool
		expectedBytes uint64
	}

	tests := []test{
		{
			name:          "SetToKiB",
			value:         "1KiB",
			envName:       "CB_TEST_ABCDEFG",
			expectedBool:  true,
			expectedBytes: 1024,
		},
		{
			name:    "UnsetEnv",
			value:   "",
			envName: "CB_TEST_ABCDEFG_amdkasd",
		},
		{
			name:    "InvalidByteString",
			value:   "7.87.232.498",
			envName: "CB_TEST_ABCDEFG",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, os.Setenv(test.envName, test.value))
			defer os.Unsetenv(test.envName)

			out, ok := GetBytes(test.envName)

			require.Equal(t, test.expectedBool, ok)
			require.Equal(t, test.expectedBytes, out)
		})
	}
}

func TestGetFloat64(t *testing.T) {
	type test struct {
		name         string
		expectedVal  float64
		expectedBool bool
		envName      string
		envValue     string
	}

	tests := []test{
		{
			name:         "ValidEnv",
			expectedVal:  1.47,
			expectedBool: true,
			envName:      "CB_TEST_ENVAR_VALID_CASE",
			envValue:     "1.47",
		},
		{
			name:    "EnvNotSet",
			envName: "CB_TEST_ENVAR_NO_ENV_CASE",
		},
		{
			name:     "EnvIsNotAFloat",
			envName:  "CB_TEST_ENVAR_NOT_AN_INT_CASE",
			envValue: "this is not a float but does have one in: 77.777",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, os.Setenv(test.envName, test.envValue))
			defer os.Unsetenv(test.envName)

			val, ok := GetFloat64(test.envName)

			require.Equal(t, test.expectedBool, ok)
			require.Equal(t, test.expectedVal, val)
		})
	}
}
