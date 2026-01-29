module github.com/couchbase/tools-common/couchbase/v4

go 1.24.0

require (
	github.com/couchbase/goutils v0.2.0
	github.com/couchbase/tools-common/auth/v2 v2.1.0
	github.com/couchbase/tools-common/environment v1.1.2
	github.com/couchbase/tools-common/errors v1.1.0
	github.com/couchbase/tools-common/http v1.0.11
	github.com/couchbase/tools-common/strings v1.0.0
	github.com/couchbase/tools-common/sync/v2 v2.0.2
	github.com/couchbase/tools-common/testing v1.0.3
	github.com/couchbase/tools-common/types/v2 v2.2.2
	github.com/couchbase/tools-common/utils/v3 v3.1.1
	// Pinned to this version as earlier versions of this library triggered
	// false positives in security checks, and there are no tagged commits
	// which include the fix. See MB-69613 for more info.
	github.com/dsnet/compress v0.0.2-0.20210315054119-f66993602bf5
	github.com/foxcpp/go-mockdns v1.0.0
	github.com/golang/snappy v1.0.0
	github.com/google/uuid v1.6.0
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.18.2
	github.com/stretchr/testify v1.11.1
	golang.org/x/crypto v0.46.0
	golang.org/x/exp v0.0.0-20251219203646-944ab1f22d93
	golang.org/x/mod v0.31.0
)

require (
	github.com/couchbase/cbauth v0.1.13 // indirect
	github.com/couchbase/clog v0.1.0 // indirect
	github.com/couchbase/go-couchbase v0.1.1 // indirect
	github.com/couchbase/gomemcached v0.3.3 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/miekg/dns v1.1.25 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Retracted due to incorrect module path (was still v3)
retract v4.0.0
