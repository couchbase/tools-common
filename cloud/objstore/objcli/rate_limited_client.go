package objcli

import (
	"context"
	"io"
	"regexp"

	"golang.org/x/time/rate"

	"github.com/couchbase/tools-common/cloud/objstore/objval"
	"github.com/couchbase/tools-common/utils/ratelimit"
)

// RateLimitedClient implements objcli.Client interface mostly by deferring to the underlying Client, but where the
// methods which involve uploading/downloading objects, the rate limiter is used to control the rate of data transfer.
//
// The rate-limited methods are:
//
// * GetObject
//
// * PutObject
//
// * AppendToObject
//
// * UploadPart
type RateLimitedClient struct {
	c Client

	rl *rate.Limiter
}

// NewRateLimitedClient returns a RateLimitedClient.
func NewRateLimitedClient(c Client, rl *rate.Limiter) *RateLimitedClient {
	return &RateLimitedClient{c: c, rl: rl}
}

func (r *RateLimitedClient) Provider() objval.Provider {
	return r.c.Provider()
}

func (r *RateLimitedClient) GetObject(ctx context.Context, bucket, key string, br *objval.ByteRange) (*objval.Object,
	error,
) {
	obj, err := r.c.GetObject(ctx, bucket, key, br)
	if err != nil {
		return nil, err
	}

	obj.Body = ratelimit.NewRateLimitedReadCloser(ctx, obj.Body, r.rl)

	return obj, nil
}

func (r *RateLimitedClient) GetObjectAttrs(ctx context.Context, bucket, key string) (*objval.ObjectAttrs, error) {
	return r.c.GetObjectAttrs(ctx, bucket, key)
}

func (r *RateLimitedClient) PutObject(ctx context.Context, bucket, key string, body io.ReadSeeker) error {
	return r.c.PutObject(ctx, bucket, key, ratelimit.NewRateLimitedReadSeeker(ctx, body, r.rl))
}

func (r *RateLimitedClient) AppendToObject(ctx context.Context, bucket, key string, data io.ReadSeeker) error {
	return r.c.AppendToObject(ctx, bucket, key, ratelimit.NewRateLimitedReadSeeker(ctx, data, r.rl))
}

func (r *RateLimitedClient) DeleteObjects(ctx context.Context, bucket string, keys ...string) error {
	return r.c.DeleteObjects(ctx, bucket, keys...)
}

func (r *RateLimitedClient) DeleteDirectory(ctx context.Context, bucket, prefix string) error {
	return r.c.DeleteDirectory(ctx, bucket, prefix)
}

func (r *RateLimitedClient) IterateObjects(ctx context.Context, bucket, prefix, delimiter string, include,
	exclude []*regexp.Regexp, fn IterateFunc,
) error {
	return r.c.IterateObjects(ctx, bucket, prefix, delimiter, include, exclude, fn)
}

func (r *RateLimitedClient) CreateMultipartUpload(ctx context.Context, bucket, key string) (string, error) {
	return r.c.CreateMultipartUpload(ctx, bucket, key)
}

func (r *RateLimitedClient) ListParts(ctx context.Context, bucket, id, key string) ([]objval.Part, error) {
	return r.c.ListParts(ctx, bucket, id, key)
}

func (r *RateLimitedClient) UploadPart(ctx context.Context, bucket, id, key string, number int,
	body io.ReadSeeker,
) (objval.Part, error) {
	return r.c.UploadPart(ctx, bucket, id, key, number, ratelimit.NewRateLimitedReadSeeker(ctx, body, r.rl))
}

func (r *RateLimitedClient) UploadPartCopy(ctx context.Context, bucket, id, dst, src string, number int,
	br *objval.ByteRange,
) (objval.Part, error) {
	return r.c.UploadPartCopy(ctx, bucket, id, dst, src, number, br)
}

func (r *RateLimitedClient) CompleteMultipartUpload(ctx context.Context, bucket, id, key string,
	parts ...objval.Part,
) error {
	return r.c.CompleteMultipartUpload(ctx, bucket, id, key, parts...)
}

func (r *RateLimitedClient) AbortMultipartUpload(ctx context.Context, bucket, id, key string) error {
	return r.c.AbortMultipartUpload(ctx, bucket, id, key)
}
