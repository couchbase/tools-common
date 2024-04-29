package ptr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetIfNil(t *testing.T) {
	type testType struct {
		i int
	}

	type test struct {
		name       string
		ptrToPtr   **testType
		otherPtr   *testType
		setToOther bool
	}

	var (
		nilPtr         *testType
		nonNilPtr      = To(testType{0})
		otherNonNilPtr = To(testType{1})
	)

	tests := []*test{
		{
			name:     "PtrToPtrNil",
			otherPtr: otherNonNilPtr,
		},
		{
			name:       "PtrNilSetToOther",
			ptrToPtr:   &nilPtr,
			otherPtr:   otherNonNilPtr,
			setToOther: true,
		},
		{
			name:     "PtrNonNilNotSet",
			ptrToPtr: &nonNilPtr,
			otherPtr: otherNonNilPtr,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.ptrToPtr == nil {
				SetIfNil(test.ptrToPtr, test.otherPtr)
				require.Nil(t, test.ptrToPtr)

				return
			}

			expectedPtr := *test.ptrToPtr

			if test.setToOther {
				expectedPtr = test.otherPtr
			}

			SetIfNil(test.ptrToPtr, test.otherPtr)

			require.Same(t, expectedPtr, *test.ptrToPtr)
		})
	}
}
