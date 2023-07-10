package util

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsMethodIdempotent(t *testing.T) {
	methods := []string{
		http.MethodGet,
		http.MethodHead,
		http.MethodPut,
		http.MethodDelete,
		http.MethodOptions,
		http.MethodTrace,
	}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			require.True(t, IsMethodIdempotent(method))
		})
	}

	t.Run(http.MethodPost, func(t *testing.T) {
		require.False(t, IsMethodIdempotent(http.MethodPost))
	})
}
