package objcli

import (
	"path"
	"regexp"
)

// ShouldIgnore uses the given regular expressions to determine if we should skip listing the provided file.
func ShouldIgnore(query string, include, exclude []*regexp.Regexp) bool {
	ignore := func(regexes []*regexp.Regexp) bool {
		for _, regex := range regexes {
			if regex.MatchString(query) || regex.MatchString(path.Base(query)) {
				return true
			}
		}

		return false
	}

	return (include != nil && !ignore(include)) || (exclude != nil && ignore(exclude))
}
