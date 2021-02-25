package log

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type argumentsTestCase struct {
	name      string
	arguments []string
	expected  []string
}

func TestUserTagArguments(t *testing.T) {
	cases := []argumentsTestCase{
		{
			name:     "nil",
			expected: []string{},
		},
		{
			name:      "empty",
			arguments: []string{},
			expected:  []string{},
		},
		{
			name:      "nothingToTag",
			arguments: []string{"-b", "--some-other-thing", "alpha", "--kilo", "5"},
			expected:  []string{"-b", "--some-other-thing", "alpha", "--kilo", "5"},
		},
		{
			name:      "tagMultiple",
			arguments: []string{"-k", "key", "-b", "--user", "carlos", "-a", "5", "--filter-keys", "key"},
			expected: []string{
				"-k", "<ud>key</ud>", "-b", "--user", "<ud>carlos</ud>", "-a", "5", "--filter-keys",
				"<ud>key</ud>",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, UserTagArguments(tc.arguments, []string{"-k", "--filter-keys", "--user"}))
		})
	}
}

func TestMaskArguments(t *testing.T) {
	cases := []argumentsTestCase{
		{
			name:     "nil",
			expected: []string{},
		},
		{
			name:      "empty",
			arguments: []string{},
			expected:  []string{},
		},
		{
			name:      "nothingToMask",
			arguments: []string{"-b", "--some-other-thing", "alpha", "--kilo", "5"},
			expected:  []string{"-b", "--some-other-thing", "alpha", "--kilo", "5"},
		},
		{
			name:      "maskFlagWithoutValue",
			arguments: []string{"-u", "user", "-p"},
			expected:  []string{"-u", "user", "-p"},
		},
		{
			name:      "maskMultiple",
			arguments: []string{"--password", "pass", "-u", "user", "-p", "p1"},
			expected:  []string{"--password", "*****", "-u", "user", "-p", "*****"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, MaskArguments(tc.arguments, []string{"-p", "--password"}))
		})
	}
}

func TestMaskAndTagArguments(t *testing.T) {
	type testCase struct {
		name      string
		arguments []string
		expected  string
	}

	cases := []testCase{
		{
			name:     "nil",
			expected: "",
		},
		{
			name:      "empty",
			arguments: []string{},
			expected:  "",
		},
		{
			name:      "nothingToMaskOrTag",
			arguments: []string{"-b", "--some-other-thing", "alpha", "--kilo", "5"},
			expected:  "-b --some-other-thing alpha --kilo 5",
		},
		{
			name:      "nothingToMask",
			arguments: []string{"-u", "user", "-b", "--kilo", "5"},
			expected:  "-u <ud>user</ud> -b --kilo 5",
		},
		{
			name:      "maskAndTag",
			arguments: []string{"--password", "pass", "-u", "user", "-p"},
			expected:  "--password ***** -u <ud>user</ud> -p",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, MaskAndUserTagArguments(tc.arguments, []string{"-u", "--user"},
				[]string{"-p", "--password"}))
		})
	}
}
