package maps

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/types/v2/ptr"
)

func TestMap(t *testing.T) {
	input := map[string]*string{"a": ptr.To("b")}

	actual := Map[map[string]*string, map[string]string](
		input,
		func(k string, v *string) (string, string) { return k, *v },
	)

	require.Equal(t, map[string]string{"a": "b"}, actual)
}
