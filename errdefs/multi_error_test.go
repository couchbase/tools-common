package errdefs

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiErrorStrings(t *testing.T) {
	type testCase struct {
		name      string
		errs      []error
		prefix    string
		separator string
		expected  string
	}

	cases := []testCase{
		{
			name:     "Empty",
			expected: "",
		},
		{
			name:      "EmptyCustomPrefixCustomSep",
			prefix:    "oh no!",
			separator: "-",
			expected:  "",
		},
		{
			name:     "One",
			errs:     []error{fmt.Errorf("A")},
			expected: "A",
		},
		{
			name:     "Three",
			errs:     []error{fmt.Errorf("A"), fmt.Errorf("B"), fmt.Errorf("C")},
			expected: "A; B; C",
		},
		{
			name:      "ThreeCustomSep",
			errs:      []error{fmt.Errorf("A"), fmt.Errorf("B"), fmt.Errorf("C")},
			separator: "-",
			expected:  "A-B-C",
		},
		{
			name:     "ThreeCustomPrefix",
			errs:     []error{fmt.Errorf("A"), fmt.Errorf("B"), fmt.Errorf("C")},
			prefix:   "oh no! ",
			expected: "oh no! A; B; C",
		},
		{
			name:      "ThreeCustomPrefixCustomSep",
			errs:      []error{fmt.Errorf("A"), fmt.Errorf("B"), fmt.Errorf("C")},
			prefix:    "oh no! ",
			separator: "-",
			expected:  "oh no! A-B-C",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			me := MultiError{
				Prefix:    tc.prefix,
				Separator: tc.separator,
			}

			for _, err := range tc.errs {
				me.Add(err)
			}
			require.Equal(t, tc.expected, me.Error())
		})
	}
}
