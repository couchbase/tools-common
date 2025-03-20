package objutil

import (
	"context"
	"errors"
	"fmt"

	"github.com/couchbase/tools-common/cloud/v7/objstore/objcli"
	"github.com/couchbase/tools-common/cloud/v7/objstore/objval"
)

// PrefixExistsOptions encapsulates the options available when running 'PrefixExists'.
type PrefixExistsOptions struct {
	// Context is the ctx.Context that can be used to cancel all requests.
	Context context.Context

	// Client is the client used to perform the operation.
	//
	// NOTE: This attribute is required.
	Client objcli.Client

	// Bucket is the bucket to upload the object to.
	//
	// NOTE: This attribute is required.
	Bucket string

	// Prefix is the prefix that is being checked.
	//
	// NOTE: This attribute is required.
	Prefix string
}

// defaults fills any missing attributes to a sane default.
func (d *PrefixExistsOptions) defaults() {
	if d.Context == nil {
		d.Context = context.Background()
	}
}

// PrefixExists returns a boolean indicating whether any objects exist in the remote provider that have the given
// prefix.
func PrefixExists(opts PrefixExistsOptions) (bool, error) {
	// Fill out any missing fields with the sane defaults
	opts.defaults()

	sentinal := errors.New("stop")

	err := opts.Client.IterateObjects(opts.Context, objcli.IterateObjectsOptions{
		Bucket:    opts.Bucket,
		Prefix:    opts.Prefix,
		Delimiter: "/",
		Func:      func(_ *objval.ObjectAttrs) error { return sentinal },
	})

	if err != nil && !errors.Is(err, sentinal) {
		return false, fmt.Errorf("failed to iterate objects: %w", err)
	}

	return errors.Is(err, sentinal), nil
}
