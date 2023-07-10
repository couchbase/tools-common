package ptr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTo(t *testing.T) {
	type testType struct{}

	var (
		testScalar = 123
		testStruct = testType{}
	)

	const testConst = "test"
	testVar := testConst

	t.Run("Scalar", func(t *testing.T) {
		require.Equal(t, To(testScalar), &testScalar)
	})

	t.Run("Struct", func(t *testing.T) {
		require.Equal(t, To(testStruct), &testStruct)
	})

	t.Run("Const", func(t *testing.T) {
		require.Equal(t, *To(testConst), testVar)
	})
}
