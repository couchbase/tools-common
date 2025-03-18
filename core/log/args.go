package log

import (
	"fmt"
	"strings"
)

var (
	cbmFlagsToMask = []string{
		"-p", "--p", "--password", "--obj-access-key-id", "--obj-secret-access-key", "--obj-refresh-token",
		"--km-access-key-id", "--km-secret-access-key", "--passphrase",
	}
	cbmFlagsToTag = []string{
		"-u", "--u", "--username", "-k", "--k", "--key", "--filter-keys", "--filter-values",
		"--km-key-url",
	}
)

// UserTagArguments returns a new slice with the values for the flags given in flagsToTag surrounded by the <ud></ud>
// tags.
func UserTagArguments(args, flagsToTag []string) []string {
	ret := make([]string, len(args))
	copy(ret, args)

	for i := 0; i < len(ret); i++ {
		if flagMatches(ret[i], flagsToTag) {
			i++

			ret[i] = fmt.Sprintf("<ud>%s</ud>", ret[i])
		}
	}

	return ret
}

func UserTagCBMArguments(args []string) []string {
	return UserTagArguments(args, cbmFlagsToTag)
}

// MaskArguments returns a new slice with the values of the flags given in flagsToMask replaced by a fix number of *.
func MaskArguments(args, flagsToMask []string) []string {
	ret := make([]string, len(args))
	copy(ret, args)

	for i := 0; i < len(ret); i++ {
		// Only mask if it matches the flagsToMask and if it has a value afterwards.
		if flagMatches(ret[i], flagsToMask) && i+1 < len(ret) && !strings.HasPrefix(ret[i+1], "-") {
			i++

			ret[i] = "*****" // Mask with fix length to avoid revealing any details about the string.
		}
	}

	return ret
}

func MaskCBMArguments(args []string) []string {
	return MaskArguments(args, cbmFlagsToMask)
}

// MaskAndUserTagArguments is a convenient way of calling both UserTagArguments and MaskArguments on the given data. It
// will return the resulting string slice joined with a space between element for easy logging.
func MaskAndUserTagArguments(args, flagsToTag, flagsToMask []string) string {
	return strings.TrimSpace(strings.Join(MaskArguments(UserTagArguments(args, flagsToTag), flagsToMask), " "))
}

func MaskAndUserTagCBMArguments(args []string) string {
	return MaskAndUserTagArguments(args, cbmFlagsToTag, cbmFlagsToMask)
}

func flagMatches(flag string, referenceFlags []string) bool {
	for _, referenceFlag := range referenceFlags {
		var (
			longFlag    = strings.HasPrefix(referenceFlag, "--")
			exactMatch  = flag == referenceFlag
			prefixMatch = strings.HasPrefix(flag, referenceFlag)
		)

		if (longFlag && exactMatch) || (!longFlag && prefixMatch) {
			return true
		}
	}

	return false
}
