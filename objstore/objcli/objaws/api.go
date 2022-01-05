package objaws

import "github.com/aws/aws-sdk-go/service/s3"

// serviceAPI is the minimal subset of functions that we use from the AWS SDK, this allows for a greatly reduce surface
// area for mock generation.
type serviceAPI interface {
	AbortMultipartUpload(*s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error)
	CompleteMultipartUpload(*s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error)
	CreateMultipartUpload(*s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error)
	DeleteObjects(*s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error)
	GetObject(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
	HeadObject(*s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	ListObjectsV2Pages(*s3.ListObjectsV2Input, func(*s3.ListObjectsV2Output, bool) bool) error
	ListPartsPages(*s3.ListPartsInput, func(*s3.ListPartsOutput, bool) bool) error
	PutObject(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
	UploadPart(*s3.UploadPartInput) (*s3.UploadPartOutput, error)
	UploadPartCopy(*s3.UploadPartCopyInput) (*s3.UploadPartCopyOutput, error)
}
