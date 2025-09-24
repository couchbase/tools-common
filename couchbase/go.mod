module github.com/couchbase/tools-common/couchbase/v3

go 1.22.0

toolchain go1.23.4

require (
	github.com/couchbase/tools-common/auth/v2 v2.0.0
	github.com/couchbase/tools-common/environment v1.1.1
	github.com/couchbase/tools-common/errors v1.0.0
	github.com/couchbase/tools-common/http v1.0.7
	github.com/couchbase/tools-common/strings v1.0.0
	github.com/couchbase/tools-common/sync/v2 v2.0.1
	github.com/couchbase/tools-common/testing v1.0.2
	github.com/couchbase/tools-common/types/v2 v2.0.1
	github.com/couchbase/tools-common/utils/v3 v3.0.2
	github.com/dsnet/compress v0.0.1
	github.com/foxcpp/go-mockdns v1.0.0
	github.com/golang/snappy v1.0.0
	github.com/google/uuid v1.6.0
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.18.0
	github.com/stretchr/testify v1.10.0
	golang.org/x/exp v0.0.0-20241204233417-43b7b7cde48d
	golang.org/x/mod v0.22.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/miekg/dns v1.1.25 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/crypto v0.30.0 // indirect
	golang.org/x/net v0.21.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Retracted in MB-63328 as the release contained an API that we weren't
// committed to supporting.
retract v3.1.0

// Retracted due to incorrect commit being tagged
retract v3.3.5
