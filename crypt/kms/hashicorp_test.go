package kms

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseHashiCorpURL(t *testing.T) {
	type testCase struct {
		name  string
		in    string
		key   string
		host  string
		valid bool
	}

	cases := []testCase{
		{
			name: "no-url",
		},
		{
			name: "no-host",
			in:   "https:///key1",
		},
		{
			name: "no-key",
			in:   "https://127.0.0.1:8200",
		},
		{
			name: "no-key",
			in:   "https://127.0.0.1:8200/",
		},
		{
			name:  "valid",
			in:    "https://127.0.0.1:8200/key_1",
			key:   "key_1",
			host:  "https://127.0.0.1:8200",
			valid: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key, host, err := parseHashiCorpURL(tc.in)
			if !tc.valid {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.key, key)
			require.Equal(t, tc.host, host)
		})
	}
}
