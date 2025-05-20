package definitions

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
		{
			name:     "Out of memory",
			errs:     []error{fmt.Errorf("A"), fmt.Errorf("B"), fmt.Errorf("C"), fmt.Errorf("testing out of memory")},
			expected: "A; B; C; error message output cap hit - not all errors are shown",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			me := MultiError{
				OutputCap: 15,
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

func TestMultiErrorErrOrNil(t *testing.T) {
	me := new(MultiError)
	require.Nil(t, me.ErrOrNil(), "received error when should have got nil")

	me.Add(fmt.Errorf("oh no"))
	require.Error(t, me.ErrOrNil(), "didn't get error when expected")
}

func TestOverflowLoop(t *testing.T) {
	var multiErr MultiError

	multiErr.Add(fmt.Errorf("oh no"))
	multiErr.Add(multiErr.ErrOrNil())

	require.Equal(t, multiErr.Error(), "oh no")
}
