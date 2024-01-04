module github.com/couchbase/tools-common/couchbase/v2

go 1.21

require (
	github.com/couchbase/tools-common/auth/v2 v2.0.0
	github.com/couchbase/tools-common/environment v1.0.4
	github.com/couchbase/tools-common/errors v1.0.0
	github.com/couchbase/tools-common/http v1.0.5
	github.com/couchbase/tools-common/strings v1.0.0
	github.com/couchbase/tools-common/sync/v2 v2.0.0
	github.com/couchbase/tools-common/testing v1.0.1
	github.com/couchbase/tools-common/types v1.1.4
	github.com/couchbase/tools-common/utils/v3 v3.0.0
	github.com/foxcpp/go-mockdns v1.0.0
	github.com/google/uuid v1.4.0
	github.com/json-iterator/go v1.1.12
	github.com/stretchr/testify v1.8.4
	golang.org/x/exp v0.0.0-20231127185646-65229373498e
	golang.org/x/mod v0.14.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/miekg/dns v1.1.25 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.19.0 // indirect
	golang.org/x/sys v0.15.0 // indirect
	golang.org/x/tools v0.16.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// The `go-mockdns` package uses v1.1.25 causing our dependencies (e.g. `x/net`) to not be upgraded, replace with the
// latest version while we wait for them to upgrade.
replace github.com/miekg/dns => github.com/miekg/dns v1.1.57
