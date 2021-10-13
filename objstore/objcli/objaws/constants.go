package objaws

const (
	// PageSize is the default page size used by AWS.
	PageSize = 1000

	// MinUploadSize is the minimum size for a multipart upload in AWS, with a little padding for paranoia sake.
	MinUploadSize = 5*1024*1024 + 1
)
