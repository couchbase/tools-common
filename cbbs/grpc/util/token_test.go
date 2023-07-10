package util

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestExtractToken(t *testing.T) {
	type testCase struct {
		name        string
		metadata    map[string]string
		expectToken string
	}

	cases := []testCase{
		{
			name: "noValue",
		},
		{
			name:     "wrongAuth",
			metadata: map[string]string{"authorization": "basic user:password"},
		},
		{
			name:        "correctToken",
			metadata:    map[string]string{"authorization": "bearer token"},
			expectToken: "token",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			md := metadata.New(tc.metadata)
			ctx := metadata.NewIncomingContext(context.Background(), md)

			token, err := ExtractToken(ctx, "bearer")
			if tc.expectToken == "" {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectToken, token)
		})
	}
}
