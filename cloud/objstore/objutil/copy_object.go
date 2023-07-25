package objutil

import (
	"fmt"
	"math"

	"github.com/couchbase/tools-common/cloud/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/objstore/objval"
	"github.com/couchbase/tools-common/types/ptr"
)

// CopyObjectOptions encapsulates the available options which can be used when copying an object.
type CopyObjectOptions struct {
	Options

	// Client is the client used to perform the operation.
	//
	// NOTE: This attribute is required.
	Client objcli.Client

	// DestinationBucket is the bucket which the copied object will be placed in.
	//
	// NOTE: This attribute is required.
	DestinationBucket string

	// DestinationKey is the key which will be used for the copied object.
	//
	// NOTE: This attribute is required.
	DestinationKey string

	// SourceBucket is the bucket in which the object being copied resides in.
	//
	// NOTE: This attribute is required.
	SourceBucket string

	// SourceKey is the key of the source object.
	//
	// NOTE: This attribute is required.
	SourceKey string
}

// CopyObject copies an object from one place to another breaking the request into multiple parts where it's known that
// cloud provider limits will be hit.
//
// NOTE: Client must have permissions to both the source/destination buckets.
func CopyObject(opts CopyObjectOptions) error {
	// Fill out any missing fields with the sane defaults
	opts.defaults()

	// Copy the variables to shorter variable name, using directly via the options structure lengthens lines too much
	var (
		ctx       = opts.Context
		client    = opts.Client
		dstBucket = opts.DestinationBucket
		dstKey    = opts.DestinationKey
		srcBucket = opts.SourceBucket
		srcKey    = opts.SourceKey
		partSize  = opts.PartSize
	)

	attrs, err := client.GetObjectAttrs(ctx, srcBucket, srcKey)
	if err != nil {
		return fmt.Errorf("failed to get object attributes: %w", err)
	}

	var (
		size = ptr.From(attrs.Size)
		max  = maxSingleOperationCopySize(client.Provider())
	)

	// If we're able to perform this operation with a single request, do that instead.
	if size <= max {
		return client.CopyObject(ctx, dstBucket, dstKey, srcBucket, srcKey)
	}

	id, err := client.CreateMultipartUpload(ctx, dstBucket, dstKey)
	if err != nil {
		return fmt.Errorf("failed to start multipart upload: %w", err)
	}
	defer client.AbortMultipartUpload(ctx, dstBucket, id, dstKey) //nolint:errcheck,wsl

	var parts []objval.Part

	// cp transfers the given range from the object into the multipart upload.
	//
	// NOTE: We currently perform this operation sequentially, so we don't need to guard access to the 'parts'. There is
	// room for improvement to do this concurrently though, so that must be considered in the future.
	cp := func(start, end int64) error {
		part, err := client.UploadPartCopy(
			ctx,
			dstBucket,
			id,
			dstKey,
			srcBucket,
			srcKey,
			len(parts)+1,
			&objval.ByteRange{Start: start, End: end},
		)
		if err != nil {
			return fmt.Errorf("failed to copy part: %w", err)
		}

		parts = append(parts, part)

		return nil
	}

	// Break the object down into chunks, and perform copy operations for each
	err = chunk(size, partSize, cp)
	if err != nil {
		return fmt.Errorf("failed to copy parts: %w", err)
	}

	err = client.CompleteMultipartUpload(ctx, dstBucket, id, dstKey, parts...)
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	return nil
}

// maxSingleOperationCopySize returns an integer representing the point at which copying must be broken down into a
// multipart upload; this is required as some cloud providers have limits on copying objects.
//
// NOTE: If the provider is unknown, a zero value is returned which will trigger multipart uploads which should always
// be valid if only slightly sub-optimal.
func maxSingleOperationCopySize(provider objval.Provider) int64 {
	switch provider {
	case objval.ProviderAWS:
		return 5 * 1000 * 1000 * 1000
	case objval.ProviderAzure:
		return 256 * 1000 * 1000
	case objval.ProviderGCP:
		// Don't trigger the multipart copy behavior for GCP; that's already handled by the SDK.
		return math.MaxInt64
	}

	return 0
}
