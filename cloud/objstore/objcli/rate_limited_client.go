package objcli

import (
	"context"

	"golang.org/x/time/rate"

	"github.com/couchbase/tools-common/cloud/v6/objstore/objval"
	"github.com/couchbase/tools-common/types/ratelimit"
)

// RateLimitedClient implements objcli.Client interface mostly by deferring to the underlying Client, but where the
// methods which involve uploading/downloading objects, the rate limiter is used to control the rate of data transfer.
//
// The rate-limited methods are:
//
// - GetObject
// - PutObject
// - AppendToObject
// - UploadPart
type RateLimitedClient struct {
	c  Client
	rl *rate.Limiter
}

// NewRateLimitedClient returns a RateLimitedClient.
func NewRateLimitedClient(c Client, rl *rate.Limiter) *RateLimitedClient {
	return &RateLimitedClient{c: c, rl: rl}
}

func (r *RateLimitedClient) Provider() objval.Provider {
	return r.c.Provider()
}

func (r *RateLimitedClient) GetObject(ctx context.Context, opts GetObjectOptions) (*objval.Object, error) {
	obj, err := r.c.GetObject(ctx, opts)
	if err != nil {
		return nil, err
	}

	obj.Body = ratelimit.NewRateLimitedReadCloser(ctx, obj.Body, r.rl)

	return obj, nil
}

func (r *RateLimitedClient) GetObjectAttrs(
	ctx context.Context,
	opts GetObjectAttrsOptions,
) (*objval.ObjectAttrs, error) {
	return r.c.GetObjectAttrs(ctx, opts)
}

func (r *RateLimitedClient) PutObject(ctx context.Context, opts PutObjectOptions) error {
	opts.Body = ratelimit.NewRateLimitedReadSeeker(ctx, opts.Body, r.rl)
	return r.c.PutObject(ctx, opts)
}

func (r *RateLimitedClient) AppendToObject(ctx context.Context, opts AppendToObjectOptions) error {
	opts.Body = ratelimit.NewRateLimitedReadSeeker(ctx, opts.Body, r.rl)
	return r.c.AppendToObject(ctx, opts)
}

func (r *RateLimitedClient) DeleteObjects(ctx context.Context, opts DeleteObjectsOptions) error {
	return r.c.DeleteObjects(ctx, opts)
}

func (r *RateLimitedClient) DeleteDirectory(ctx context.Context, opts DeleteDirectoryOptions) error {
	return r.c.DeleteDirectory(ctx, opts)
}

func (r *RateLimitedClient) IterateObjects(ctx context.Context, opts IterateObjectsOptions) error {
	return r.c.IterateObjects(ctx, opts)
}

func (r *RateLimitedClient) CreateMultipartUpload(
	ctx context.Context,
	opts CreateMultipartUploadOptions,
) (string, error) {
	return r.c.CreateMultipartUpload(ctx, opts)
}

func (r *RateLimitedClient) ListParts(ctx context.Context, opts ListPartsOptions) ([]objval.Part, error) {
	return r.c.ListParts(ctx, opts)
}

func (r *RateLimitedClient) UploadPart(ctx context.Context, opts UploadPartOptions) (objval.Part, error) {
	opts.Body = ratelimit.NewRateLimitedReadSeeker(ctx, opts.Body, r.rl)
	return r.c.UploadPart(ctx, opts)
}

func (r *RateLimitedClient) UploadPartCopy(ctx context.Context, opts UploadPartCopyOptions) (objval.Part, error) {
	return r.c.UploadPartCopy(ctx, opts)
}

func (r *RateLimitedClient) CompleteMultipartUpload(ctx context.Context, opts CompleteMultipartUploadOptions) error {
	return r.c.CompleteMultipartUpload(ctx, opts)
}

func (r *RateLimitedClient) AbortMultipartUpload(ctx context.Context, opts AbortMultipartUploadOptions) error {
	return r.c.AbortMultipartUpload(ctx, opts)
}
