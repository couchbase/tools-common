module github.com/couchbase/tools-common/utils/v2

go 1.18

require (
	github.com/couchbase/tools-common/core v1.0.0
	github.com/couchbase/tools-common/strings v1.0.0
	github.com/stretchr/testify v1.8.4
	golang.org/x/exp v0.0.0-20231110203233-9a3e6036ecaa
	golang.org/x/sys v0.14.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Not correctly setup for V2 versioning, see https://go.dev/ref/mod#major-version-suffixes.
retract v2.0.0

// Contained a breaking change which was subsequently discovered to be incomplete, see MB-59660 for more information.
retract v2.0.1
