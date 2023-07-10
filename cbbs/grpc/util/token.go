// Package util provides gRPC utilties used by 'cbbs'.
package util

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"
)

// ExtractToken gets an auth token from the context of a gRPC call.
func ExtractToken(ctx context.Context, scheme string) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("no metadata found")
	}

	auths := md.Get("authorization")
	if len(auths) == 0 {
		return "", fmt.Errorf("no authorization headers")
	}

	authTokenParts := strings.SplitN(auths[0], " ", 2)
	if len(authTokenParts) < 2 {
		return "", fmt.Errorf("invalid auth token")
	}

	if !strings.EqualFold(scheme, authTokenParts[0]) {
		return "", fmt.Errorf("invalid auth scheme")
	}

	return authTokenParts[1], nil
}
