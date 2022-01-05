package objgcp

import (
	"fmt"
	"regexp"
)

// regexUUID is an uncompiled regular expression which matches a standard uuid.
const regexUUID = `[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}`

// RegexUploadPart matches the key for an object created by the GCP client as part of emulating multipart uploads.
var RegexUploadPart = regexp.MustCompile(fmt.Sprintf(`^.*-mpu-%s-%s$`, regexUUID, regexUUID))
