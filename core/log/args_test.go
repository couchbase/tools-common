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
		{
			name:      "tagInlineValue",
			arguments: []string{"--user=carlos", "-a", "5", "--user-agent=x"},
			expected:  []string{"--user=<ud>carlos</ud>", "-a", "5", "--user-agent=x"},
		},
		{
			name:      "tagIgnoringDashCount",
			arguments: []string{"-user", "carlos", "--k", "key", "-filter-keys", "f", "-kilo", "5"},
			expected:  []string{"-user", "<ud>carlos</ud>", "--k", "<ud>key</ud>", "-filter-keys", "<ud>f</ud>", "-kilo", "5"},
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
			"-u", "<ud>username</ud>", "--username", "<ud>username</ud>", "--filter-keys", "<ud>filter</ud>",
			"--filter-values", "<ud>vals</ud>", "--km-key-url", "<ud>url</ud>",
		},
		UserTagCBMArguments([]string{
			"-u", "username", "--username", "username", "--filter-keys", "filter", "--filter-values", "vals",
			"--km-key-url", "url",
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
		{
			name:      "maskInlineValue",
			arguments: []string{"--password=a=b", "--p=", "--password-file", "f", "--period=1"},
			expected:  []string{"--password=*****", "--p=*****", "--password-file", "f", "--period=1"},
		},
		{
			name:      "maskValueStartingWithDash",
			arguments: []string{"--password", "-pass", "-u", "user"},
			expected:  []string{"--password", "*****", "-u", "user"},
		},
		{
			name:      "maskIgnoringDashCount",
			arguments: []string{"-password", "pass", "--p", "p1", "--p=p2", "-period", "1", "-ppass", "x"},
			expected:  []string{"-password", "*****", "--p", "*****", "--p=*****", "-period", "1", "-ppass", "x"},
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
			"--km-refresh-token", "*****", "--passphrase", "*****", "--auth-token", "*****", "--salt", "*****",
			"--client-cert-password", "*****", "--client-key-password", "*****", "-k", "*****", "--key", "*****",
			"--collection-string", "*****", "--bucket", "*****", "--include-data", "*****", "--exclude-data", "*****",
			"--include-buckets", "*****", "--exclude-buckets", "*****",
		},
		MaskCBMArguments([]string{
			"-p", "pass", "--password", "pass", "--p", "pass", "--obj-access-key-id", "keyid",
			"--obj-secret-access-key", "secret", "--obj-refresh-token", "token", "--km-secret-access-key", "secret",
			"--km-refresh-token", "token", "--passphrase", "pass", "--auth-token", "alongtoken", "--salt", "salt",
			"--client-cert-password", "pass", "--client-key-password", "pass", "-k", "key", "--key", "key",
			"--collection-string", "b.s.c", "--bucket", "b", "--include-data", "b.s", "--exclude-data", "b.t",
			"--include-buckets", "b1", "--exclude-buckets", "b2",
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
			"***** --key ***** --filter-keys <ud>filter</ud> --filter-values <ud>vals</ud> --km-key-url "+
			"<ud>url</ud> --k ***** --u <ud>username</ud>",
		MaskAndUserTagCBMArguments([]string{
			"-p", "pass", "--password", "pass", "--obj-access-key-id", "keyid",
			"--obj-secret-access-key", "secret", "--obj-refresh-token", "token", "--km-secret-access-key", "secret",
			"--passphrase", "pass", "-u", "username", "--username", "username", "-k", "key", "--key", "key", "--filter-keys",
			"filter", "--filter-values", "vals", "--km-key-url", "url", "--k", "key", "--u", "username",
		}))
}

func TestFlagMatches(t *testing.T) {
	type testCase struct {
		name     string
		arg      string
		expected bool
	}

	cases := []testCase{
		{name: "longFlag", arg: "--password", expected: true},
		{name: "shortFlag", arg: "-p", expected: true},
		{name: "longFlagSingleDash", arg: "-password", expected: true},
		{name: "shortFlagDoubleDash", arg: "--p", expected: true},
		{name: "shortFlagPrefixOfLongerFlag", arg: "-period"},
		{name: "shortFlagWithValueAttached", arg: "-ppass"},
		{name: "longFlagPrefixOfLongerFlag", arg: "--password-file"},
		{name: "otherFlag", arg: "--username"},
		{name: "notAFlag", arg: "password"},
		{name: "onlyDashes", arg: "--"},
		{name: "empty", arg: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, flagMatches(tc.arg, []string{"-p", "--password"}))
		})
	}
}

func TestCutInlineValue(t *testing.T) {
	type testCase struct {
		name   string
		arg    string
		flag   string
		value  string
		inline bool
	}

	cases := []testCase{
		{name: "inlineValue", arg: "--password=pass", flag: "--password", value: "pass", inline: true},
		{name: "valueContainingEquals", arg: "--password=a=b", flag: "--password", value: "a=b", inline: true},
		{name: "emptyValue", arg: "--p=", flag: "--p", inline: true},
		{name: "singleDash", arg: "-p=pass", flag: "-p=pass"},
		{name: "noValue", arg: "--password", flag: "--password"},
		{name: "notAFlag", arg: "a=b", flag: "a=b"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flag, value, inline := cutInlineValue(tc.arg)
			require.Equal(t, tc.flag, flag)
			require.Equal(t, tc.value, value)
			require.Equal(t, tc.inline, inline)
		})
	}
}

func TestMaskAndTagCBMArgumentsSingleDash(t *testing.T) {
	require.Equal(t,
		"-obj-access-key-id ***** -obj-secret-access-key ***** -obj-refresh-token ***** -km-access-key-id ***** "+
			"-km-secret-access-key ***** -km-refresh-token ***** -auth-token ***** -salt ***** -client-cert-password "+
			"***** -client-key-password ***** -passphrase ***** -password ***** -username <ud>username</ud> -key "+
			"***** -km-key-url <ud>url</ud>",
		MaskAndUserTagCBMArguments([]string{
			"-obj-access-key-id", "keyid", "-obj-secret-access-key", "secret", "-obj-refresh-token", "token",
			"-km-access-key-id", "keyid", "-km-secret-access-key", "secret", "-km-refresh-token", "token",
			"-auth-token", "token", "-salt", "salt", "-client-cert-password", "pass", "-client-key-password", "pass",
			"-passphrase", "pass", "-password", "pass", "-username", "username", "-key", "key", "-km-key-url", "url",
		}))
}
