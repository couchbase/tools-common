package log

import (
	"fmt"
	"strings"
)

var (
	cbmFlagsToMask = []string{
		"-p", "--p", "--password", "--obj-access-key-id", "--obj-secret-access-key", "--obj-refresh-token",
		"--km-access-key-id", "--km-secret-access-key", "--km-refresh-token", "--passphrase", "--auth-token", "--salt",
		"--client-cert-password", "--client-key-password", "-k", "--k", "--key", "--collection-string", "--bucket",
		"--include-data", "--exclude-data", "--include-buckets", "--exclude-buckets",
	}
	cbmFlagsToTag = []string{
		"-u", "--u", "--username", "--filter-keys", "--filter-values", "--km-key-url",
	}
)

// UserTagArguments returns a new slice with the values for the flags given in flagsToTag surrounded by the <ud></ud>
// tags. Flags match regardless of their number of leading dashes, and only flags with two dashes can have their value
// attached with '=', as in cbflag.
func UserTagArguments(args, flagsToTag []string) []string {
	ret := make([]string, len(args))
	copy(ret, args)

	for i := 0; i < len(ret); i++ {
		flag, value, inline := cutInlineValue(ret[i])
		if !flagMatches(flag, flagsToTag) {
			continue
		}

		if inline {
			ret[i] = fmt.Sprintf("%s=<ud>%s</ud>", flag, value)
			continue
		}

		i++

		ret[i] = fmt.Sprintf("<ud>%s</ud>", ret[i])
	}

	return ret
}

func UserTagCBMArguments(args []string) []string {
	return UserTagArguments(args, cbmFlagsToTag)
}

// MaskArguments returns a new slice with the values of the flags given in flagsToMask replaced by a fix number of *.
// Flags match regardless of their number of leading dashes, and only flags with two dashes can have their value
// attached with '=', as in cbflag.
func MaskArguments(args, flagsToMask []string) []string {
	ret := make([]string, len(args))
	copy(ret, args)

	for i := 0; i < len(ret); i++ {
		flag, _, inline := cutInlineValue(ret[i])
		if !flagMatches(flag, flagsToMask) {
			continue
		}

		if inline {
			ret[i] = flag + "=*****" // Mask with fix length to avoid revealing any details about the string.
			continue
		}

		// Only mask if it has a value afterwards.
		if i+1 < len(ret) {
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

// flagMatches reports whether 'flag' is one of 'referenceFlags'. The number of leading dashes doesn't matter; we do
// this to match cbflag behaviour. For example, with 'referenceFlags' of '--password' and '-p':
//   - '--password', '-password', '-p' and '--p' match.
//   - '--password-file', '-period' and 'password' don't match.
func flagMatches(flag string, referenceFlags []string) bool {
	if !strings.HasPrefix(flag, "-") {
		return false
	}

	name := strings.TrimLeft(flag, "-")

	for _, referenceFlag := range referenceFlags {
		if name == strings.TrimLeft(referenceFlag, "-") {
			return true
		}
	}

	return false
}

// cutInlineValue splits 'arg' around the first '=' if it's a flag with its value attached, returning the flag, the
// value and true. Otherwise, it returns 'arg', an empty string and false. Only flags with two dashes can have their
// value attached; we do this to match cbflag behaviour. For example:
//   - '--password=pass' gives '--password', 'pass' and true.
//   - '--password', '-p=pass' and 'a=b' are returned as they are, along with "" and false.
func cutInlineValue(arg string) (string, string, bool) {
	flag, value, ok := strings.Cut(arg, "=")
	if !ok || !strings.HasPrefix(flag, "--") {
		return arg, "", false
	}

	return flag, value, true
}
