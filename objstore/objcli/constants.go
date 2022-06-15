package objcli

const (
	// NoUploadID is a readability definition which should be used by client implementations for cloud providers which
	// do not utilize uploads ids.
	NoUploadID = ""

	// NoPartNumber is a readability definition which should be used by client implementations for cloud providers that
	// do not need numbers to order parts for multipart uploads.
	NoPartNumber = 0
)
