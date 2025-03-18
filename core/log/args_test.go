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

func TestUserTagCBMArguments(t *testing.T) {
	require.Equal(t,
		[]string{
			"-u", "<ud>username</ud>", "--username", "<ud>username</ud>", "-k", "<ud>key</ud>", "--key",
			"<ud>key</ud>", "--filter-keys", "<ud>filter</ud>", "--filter-values", "<ud>vals</ud>", "--km-key-url",
			"<ud>url</ud>",
		},
		UserTagCBMArguments([]string{
			"-u", "username", "--username", "username", "-k", "key", "--key", "key",
			"--filter-keys", "filter", "--filter-values", "vals", "--km-key-url", "url",
		}),
	)
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
		{
			name: "doNotMaskStartingWithP",
			arguments: []string{
				"--password", "pass", "-u", "user", "-p", "p1", "--p", "aaa", "--period", "123", "-P", "123",
			},
			expected: []string{
				"--password", "*****", "-u", "user", "-p", "*****", "--p", "*****", "--period", "123", "-P", "123",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, MaskArguments(tc.arguments, []string{"-p", "--password", "--p"}))
		})
	}
}

func TestMaskCBMArguments(t *testing.T) {
	require.Equal(t,
		[]string{
			"-p", "*****", "--password", "*****", "--p", "*****", "--obj-access-key-id", "*****",
			"--obj-secret-access-key", "*****", "--obj-refresh-token", "*****", "--km-secret-access-key", "*****",
			"--passphrase", "*****",
		},
		MaskCBMArguments([]string{
			"-p", "pass", "--password", "pass", "--p", "pass", "--obj-access-key-id", "keyid",
			"--obj-secret-access-key", "secret", "--obj-refresh-token", "token", "--km-secret-access-key", "secret",
			"--passphrase", "pass",
		}))
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

func TestMaskAndTagCBMArguments(t *testing.T) {
	require.Equal(t,
		"-p ***** --password ***** --obj-access-key-id ***** --obj-secret-access-key ***** --obj-refresh-token ***** "+
			"--km-secret-access-key ***** --passphrase ***** -u <ud>username</ud> --username <ud>username</ud> -k "+
			"<ud>key</ud> --key <ud>key</ud> --filter-keys <ud>filter</ud> --filter-values <ud>vals</ud> --km-key-url "+
			"<ud>url</ud> --k <ud>key</ud> --u <ud>username</ud>",
		MaskAndUserTagCBMArguments([]string{
			"-p", "pass", "--password", "pass", "--obj-access-key-id", "keyid",
			"--obj-secret-access-key", "secret", "--obj-refresh-token", "token", "--km-secret-access-key", "secret",
			"--passphrase", "pass", "-u", "username", "--username", "username", "-k", "key", "--key", "key", "--filter-keys",
			"filter", "--filter-values", "vals", "--km-key-url", "url", "--k", "key", "--u", "username",
		}))
}
