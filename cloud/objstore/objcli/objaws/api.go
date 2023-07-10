package objaws

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

//go:generate mockery --all --case underscore --inpackage

// serviceAPI is the minimal subset of functions that we use from the AWS SDK, this allows for a greatly reduce surface
// area for mock generation.
type serviceAPI interface {
	AbortMultipartUploadWithContext(
		context.Context, *s3.AbortMultipartUploadInput, ...request.Option,
	) (*s3.AbortMultipartUploadOutput, error)

	CompleteMultipartUploadWithContext(
		context.Context, *s3.CompleteMultipartUploadInput, ...request.Option,
	) (*s3.CompleteMultipartUploadOutput, error)

	CreateMultipartUploadWithContext(
		context.Context, *s3.CreateMultipartUploadInput, ...request.Option,
	) (*s3.CreateMultipartUploadOutput, error)

	DeleteObjectsWithContext(context.Context, *s3.DeleteObjectsInput, ...request.Option) (*s3.DeleteObjectsOutput, error)
	GetObjectWithContext(context.Context, *s3.GetObjectInput, ...request.Option) (*s3.GetObjectOutput, error)
	HeadObjectWithContext(context.Context, *s3.HeadObjectInput, ...request.Option) (*s3.HeadObjectOutput, error)

	ListObjectsV2PagesWithContext(
		context.Context, *s3.ListObjectsV2Input, func(*s3.ListObjectsV2Output, bool) bool, ...request.Option,
	) error

	ListPartsPagesWithContext(
		context.Context, *s3.ListPartsInput, func(*s3.ListPartsOutput, bool) bool, ...request.Option,
	) error

	PutObjectWithContext(context.Context, *s3.PutObjectInput, ...request.Option) (*s3.PutObjectOutput, error)
	UploadPartWithContext(context.Context, *s3.UploadPartInput, ...request.Option) (*s3.UploadPartOutput, error)

	UploadPartCopyWithContext(
		context.Context, *s3.UploadPartCopyInput, ...request.Option,
	) (*s3.UploadPartCopyOutput, error)
}
