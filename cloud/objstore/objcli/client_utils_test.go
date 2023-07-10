package objcli

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldIgnore(t *testing.T) {
	type test struct {
		name             string
		query            string
		include, exclude []*regexp.Regexp
		expected         bool
	}

	tests := []*test{
		{
			name:    "IncludeOnPath",
			query:   "/path/to/object",
			include: []*regexp.Regexp{regexp.MustCompile(regexp.QuoteMeta("/path/to/object"))},
		},
		{
			name:    "IncludeOnBasename",
			query:   "/path/to/object",
			include: []*regexp.Regexp{regexp.MustCompile("^object$")},
		},
		{
			name:     "IncludeNoneMatched",
			query:    "/path/to/object",
			include:  []*regexp.Regexp{},
			expected: true,
		},
		{
			name:     "ExcludeOnPath",
			query:    "/path/to/object",
			exclude:  []*regexp.Regexp{regexp.MustCompile(regexp.QuoteMeta("/path/to/object"))},
			expected: true,
		},
		{
			name:     "ExcludeOnBasename",
			query:    "/path/to/object",
			exclude:  []*regexp.Regexp{regexp.MustCompile("^object$")},
			expected: true,
		},
		{
			name:    "ExcludeNoneMatched",
			query:   "/path/to/object",
			exclude: []*regexp.Regexp{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, ShouldIgnore(test.query, test.include, test.exclude))
		})
	}
}
