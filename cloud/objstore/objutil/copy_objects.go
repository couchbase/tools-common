package objutil

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/couchbase/tools-common/cloud/v2/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v2/objstore/objval"
	"github.com/couchbase/tools-common/core/log"
	"github.com/couchbase/tools-common/sync/hofp"
)

// CopyObjectsOptions encapsulates the available options which can be used when copying objects from one prefix to
// another.
type CopyObjectsOptions struct {
	Options

	// Client is the client used to perform the operation.
	//
	// NOTE: This attribute is required.
	Client objcli.Client

	// DestinationBucket is the bucket which the copied object will be placed in.
	//
	// NOTE: This attribute is required.
	DestinationBucket string

	// DestinationPrefix is the prefix under which all the objects will be copied to.
	//
	// NOTE: This attribute is required.
	DestinationPrefix string

	// SourceBucket is the bucket in which the object being copied resides in.
	//
	// NOTE: This attribute is required.
	SourceBucket string

	// SourcePrefix is the prefix which will be copied.
	//
	// NOTE: This attribute is required.
	SourcePrefix string

	// SourceDelimiter is the delimiter used when listing, allowing listing/copying of only a single directory.
	SourceDelimiter string

	// SourceInclude allows selecting keys which only match any of the given expressions.
	SourceInclude []*regexp.Regexp

	// SourceExclude allows skipping keys which may any of the given expressions.
	SourceExclude []*regexp.Regexp

	// Logger is the logger that'll be used.
	Logger log.Logger
}

// CopyObjects from one location to another using a worker pool.
//
// NOTE: When copying within the same bucket, the source/destination prefix can't be the same.
func CopyObjects(opts CopyObjectsOptions) error {
	// Fill out any missing fields with the sane defaults
	opts.defaults()

	// Don't allow users to copy to the same prefix, it doesn't make sense
	if opts.SourceBucket == opts.DestinationBucket && opts.SourcePrefix == opts.DestinationPrefix {
		return ErrCopyToSamePrefix
	}

	pool := hofp.NewPool(hofp.Options{
		Context:   opts.Context,
		LogPrefix: "(objutil)",
		Logger:    log.NewWrappedLogger(opts.Logger),
	})

	cp := func(ctx context.Context, attrs *objval.ObjectAttrs) error {
		if attrs.IsDir() {
			return nil
		}

		options := CopyObjectOptions{
			Options:           opts.Options.WithContext(ctx),
			Client:            opts.Client,
			DestinationBucket: opts.DestinationBucket,
			DestinationKey:    strings.Replace(attrs.Key, opts.SourcePrefix, opts.DestinationPrefix, 1),
			SourceBucket:      opts.SourceBucket,
			SourceKey:         attrs.Key,
		}

		return CopyObject(options)
	}

	queue := func(attrs *objval.ObjectAttrs) error {
		return pool.Queue(func(ctx context.Context) error { return cp(ctx, attrs) })
	}

	err := opts.Client.IterateObjects(context.Background(), objcli.IterateObjectsOptions{
		Bucket:    opts.SourceBucket,
		Prefix:    opts.SourcePrefix,
		Delimiter: opts.SourceDelimiter,
		Include:   opts.SourceInclude,
		Exclude:   opts.SourceExclude,
		Func:      queue,
	})
	if err != nil {
		return fmt.Errorf("failed to iterate objects: %w", err)
	}

	err = pool.Stop()
	if err != nil {
		return fmt.Errorf("failed to stop worker pool: %w", err)
	}

	return nil
}
