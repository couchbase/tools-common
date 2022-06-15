package ptrutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToPtr(t *testing.T) {
	type testType struct{}

	var (
		testScalar = 123
		testStruct = testType{}
	)

	const testConst = "test"
	testVar := testConst

	t.Run("Scalar", func(t *testing.T) {
		require.Equal(t, ToPtr(testScalar), &testScalar)
	})

	t.Run("Struct", func(t *testing.T) {
		require.Equal(t, ToPtr(testStruct), &testStruct)
	})

	t.Run("Const", func(t *testing.T) {
		require.Equal(t, *ToPtr(testConst), testVar)
	})
}

func TestSetPtrIfNil(t *testing.T) {
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
		nonNilPtr      = ToPtr(testType{0})
		otherNonNilPtr = ToPtr(testType{1})
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
				SetPtrIfNil(test.ptrToPtr, test.otherPtr)
				require.Nil(t, test.ptrToPtr)
				return
			}

			expectedPtr := *test.ptrToPtr

			if test.setToOther {
				expectedPtr = test.otherPtr
			}

			SetPtrIfNil(test.ptrToPtr, test.otherPtr)

			require.Same(t, expectedPtr, *test.ptrToPtr)
		})
	}
}
