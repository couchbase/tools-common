package objaws

const (
	// PageSize is the default page size used by AWS.
	PageSize = 1_000

	// MaxUploadParts is the maximum number of parts for a multipart upload in AWS.
	MaxUploadParts = 10_000

	// MinUploadSize is the minimum size for a multipart upload in AWS.
	MinUploadSize = 5 * 1024 * 1024
)
