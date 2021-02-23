package env

import (
	"os"
	"testing"
	"time"
)

func failIfError(t *testing.T, err error, msg string) {
	if err != nil {
		t.Fatalf(msg + " ERROR: " + err.Error())
	}
}

func TestGetIntEnvVar(t *testing.T) {
	type TestCase struct {
		Name         string
		ExpectedVal  int
		ExpectedBool bool
		EnvName      string
		EnvValue     string
	}

	testcases := []TestCase{
		{
			Name:         "Valid-Case",
			ExpectedVal:  10,
			ExpectedBool: true,
			EnvName:      "CB_TEST_ENVAR_VALID_CASE",
			EnvValue:     "10",
		},
		{
			Name:    "No-Env-Var-Case",
			EnvName: "CB_TEST_ENVAR_NO_ENV_CASE",
		},
		{
			Name:     "Not-An-Int-Var-Case",
			EnvName:  "CB_TEST_ENVAR_NOT_AN_INT_CASE",
			EnvValue: "this is not an int",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			err := os.Setenv(tc.EnvName, tc.EnvValue)
			failIfError(t, err, "could not set env var line 45")

			val, ok := GetIntEnvVar(tc.EnvName)
			os.Unsetenv(tc.EnvName)

			if ok != tc.ExpectedBool {
				t.Fatalf("Expected ok to be %v got %v", tc.ExpectedBool, ok)
			}

			if val != tc.ExpectedVal {
				t.Fatalf("Expected value to be %v got %v", tc.ExpectedVal, val)
			}
		})
	}
}

func TestGetUint64EnvVar(t *testing.T) {
	type TestCase struct {
		Name         string
		ExpectedVal  uint64
		ExpectedBool bool
		EnvName      string
		EnvValue     string
	}

	testcases := []TestCase{
		{
			Name:         "Valid-Case",
			ExpectedVal:  10,
			ExpectedBool: true,
			EnvName:      "CB_TEST_ENVAR_VALID_CASE",
			EnvValue:     "10",
		},
		{
			Name:    "No-Env-Var-Case",
			EnvName: "CB_TEST_ENVAR_NO_ENV_CASE",
		},
		{
			Name:     "Not-An-Int-Var-Case",
			EnvName:  "CB_TEST_ENVAR_NOT_AN_INT_CASE",
			EnvValue: "this is not an int",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			err := os.Setenv(tc.EnvName, tc.EnvValue)
			failIfError(t, err, "could not set env var line 45")

			val, ok := GetUint64EnvVar(tc.EnvName)
			os.Unsetenv(tc.EnvName)

			if ok != tc.ExpectedBool {
				t.Fatalf("Expected ok to be %v got %v", tc.ExpectedBool, ok)
			}

			if val != tc.ExpectedVal {
				t.Fatalf("Expected value to be %v got %v", tc.ExpectedVal, val)
			}
		})
	}
}

func TestGetStringEnvVar(t *testing.T) {
	// test case where there is no envar
	key := "CB_THIS_ENV_VAR_SHOULD_NOT_EXIST"
	os.Unsetenv(key)

	_, ok := GetStringEnvVar("")
	if ok {
		t.Fatalf("It should not be able to get a env var that is not there")
	}

	key = "CB_ENV_VAR_TEST_VALID_STRING_ENV_VAR"
	setVal := "A string can be anything"
	err := os.Setenv(key, setVal)
	failIfError(t, err, "could not set env var line 72")

	val, ok := GetStringEnvVar(key)

	os.Unsetenv(key)

	if !ok {
		t.Fatalf("Should be able to retrieve key")
	}

	if val != setVal {
		t.Fatalf("Expected %s got %s", setVal, val)
	}
}

func TestGetDurationEnvVar(t *testing.T) {
	type testCase struct {
		name             string
		value            string
		envName          string
		expectedBool     bool
		expectedDuration time.Duration
	}

	tests := []testCase{
		{
			name:             "set-to-seconds",
			value:            "1s",
			envName:          "CB_TEST_ABCDEFG",
			expectedBool:     true,
			expectedDuration: time.Second,
		},
		{
			name:             "set-to-seconds-and-minutes",
			value:            "33m1s",
			envName:          "CB_TEST_ABCDEFG",
			expectedBool:     true,
			expectedDuration: 33*time.Minute + 1*time.Second,
		},
		{
			name:             "unset-env",
			value:            "",
			envName:          "CB_TEST_ABCDEFG_amdkasd",
			expectedBool:     false,
			expectedDuration: 0,
		},
		{
			name:             "invalid-time-string",
			value:            "7.87.232.498",
			envName:          "CB_TEST_ABCDEFG",
			expectedBool:     false,
			expectedDuration: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := os.Setenv(tc.envName, tc.value)
			if err != nil {
				t.Fatalf("unexpected error setting env var")
			}

			out, ok := GetDurationEnvVar(tc.envName)
			if ok != tc.expectedBool {
				t.Fatalf("Expected ok to be : %v got %v", tc.expectedBool, ok)
			}

			if out != tc.expectedDuration {
				t.Fatalf("Expected duration to be : %v got %v", tc.expectedDuration, out)
			}
		})
	}
}
