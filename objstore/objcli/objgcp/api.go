package objgcp

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

// storageAPI is a top level interface which allows interactions with Google cloud storage.
type serviceAPI interface {
	Bucket(name string) bucketAPI
}

// serviceClient implements the 'storageAPI' interface and encapsulates the Google SDK into a unit testable interface.
type serviceClient struct {
	c *storage.Client
}

func (s serviceClient) Bucket(name string) bucketAPI {
	return bucketHandle{h: s.c.Bucket(name)}
}

// bucketAPI is a bucket level interface which allows interactions with a Google Storage bucket.
type bucketAPI interface {
	Object(key string) objectAPI
	Objects(ctx context.Context, query *storage.Query) objectIteratorAPI
}

// bucketHandle implements the 'bucketAPI' interface and encapsulates the Google Storage SDK into a unit testable
// interface.
type bucketHandle struct {
	h *storage.BucketHandle
}

func (b bucketHandle) Object(key string) objectAPI {
	return &objectHandle{h: b.h.Object(key)}
}

func (b bucketHandle) Objects(ctx context.Context, query *storage.Query) objectIteratorAPI {
	return b.h.Objects(ctx, query)
}

// objectAPI is an object level API which allows interactions with an object stored in a Google cloud bucket.
type objectAPI interface {
	Attrs(ctx context.Context) (*storage.ObjectAttrs, error)
	Delete(ctx context.Context) error
	NewRangeReader(ctx context.Context, offset, length int64) (readerAPI, error)
	NewWriter(ctx context.Context) writerAPI
	ComposerFrom(srcs ...objectAPI) composeAPI
	CopierFrom(src objectAPI) copierAPI
}

// objectHandle implements the 'objectAPI' interface and encapsulates the Google Storage SDK into a unit testable
// interface.
type objectHandle struct {
	h *storage.ObjectHandle
}

func (o objectHandle) Attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	return o.h.Attrs(ctx)
}

func (o objectHandle) Delete(ctx context.Context) error {
	return o.h.Delete(ctx)
}

func (o objectHandle) NewRangeReader(ctx context.Context, offset, length int64) (readerAPI, error) {
	r, err := o.h.NewRangeReader(ctx, offset, length)
	if err != nil {
		return nil, err
	}

	return &reader{r: r}, nil
}

func (o objectHandle) NewWriter(ctx context.Context) writerAPI {
	writer := &writer{w: o.h.NewWriter(ctx)}

	// Disable SDK upload chunking
	writer.w.ChunkSize = 0

	return writer
}

func (o objectHandle) ComposerFrom(srcs ...objectAPI) composeAPI {
	converted := make([]*storage.ObjectHandle, 0, len(srcs))
	for _, src := range srcs {
		converted = append(converted, src.(*objectHandle).h)
	}

	return &composer{c: o.h.ComposerFrom(converted...)}
}

func (o objectHandle) CopierFrom(src objectAPI) copierAPI {
	return &copier{c: o.h.CopierFrom(src.(*objectHandle).h)}
}

// readerAPI is a range aware reader API which is used to stream object data from Google Storage.
type readerAPI interface {
	io.ReadCloser
	Attrs() storage.ReaderObjectAttrs
}

// reader implements the 'readerAPI' and encapsulates the Google Storage SDK into a unit testable interface.
type reader struct {
	r *storage.Reader
}

func (r reader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

func (r reader) Close() error {
	return r.r.Close()
}

func (r reader) Attrs() storage.ReaderObjectAttrs {
	return r.r.Attrs
}

// writerAPI is a checksum aware writer API which is used to upload data to Google Storage.
type writerAPI interface {
	io.WriteCloser
	SendMD5(md5 []byte)
	SendCRC(crc uint32)
}

// writer implements the 'writerAPI' and encapsulates the Google Storage SDK into a unit testable interface.
type writer struct {
	w *storage.Writer
}

func (w writer) Write(p []byte) (int, error) {
	return w.w.Write(p)
}

func (w writer) Close() error {
	return w.w.Close()
}

func (w writer) SendMD5(md5 []byte) {
	w.w.ObjectAttrs.MD5 = md5
}

func (w writer) SendCRC(crc uint32) {
	w.w.SendCRC32C = true
	w.w.ObjectAttrs.CRC32C = crc
}

// objectIteratorAPI is an object level iterator API which can be used to list objects in Google Storage.
type objectIteratorAPI interface {
	Next() (*storage.ObjectAttrs, error)
}

// composeAPI object level API which allows composing objects from up to 32 (the current maximum) individual objects and
// can be thought of as poor mans multipart uploads.
//
// NOTE: Google Storage does support resumable streaming uploads, however, the SDK doesn't expose this functionality in
// a way which would work in a way which we'd desire. For example, no API is exposed to save/maintain upload state to
// allow resuming after a process has died (required for resume).
type composeAPI interface {
	Run(ctx context.Context) (*storage.ObjectAttrs, error)
}

// composer implements the 'composeAPI' interface and encapsulates the Google Storage SDK in a unit testable interface.
type composer struct {
	c *storage.Composer
}

func (c composer) Run(ctx context.Context) (*storage.ObjectAttrs, error) {
	return c.c.Run(ctx)
}

type copierAPI interface {
	Run(ctx context.Context) (*storage.ObjectAttrs, error)
}

type copier struct {
	c *storage.Copier
}

func (c copier) Run(ctx context.Context) (*storage.ObjectAttrs, error) {
	return c.c.Run(ctx)
}
