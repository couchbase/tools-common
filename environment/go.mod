module github.com/couchbase/tools-common/environment

go 1.18

require (
	github.com/couchbase/tools-common/http v0.0.0-00010101000000-000000000000
	github.com/couchbase/tools-common/strings v0.0.0-00010101000000-000000000000
	github.com/couchbase/tools-common/types v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.8.4
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Until v0.1.0 of each 'tools-common' module is released retain the current
// behavior and use the local version.
replace (
	github.com/couchbase/tools-common/auth => ../auth
	github.com/couchbase/tools-common/cbbs => ../cbbs
	github.com/couchbase/tools-common/cloud => ../cloud
	github.com/couchbase/tools-common/core => ../core
	github.com/couchbase/tools-common/couchbase => ../coucbhase
	github.com/couchbase/tools-common/databases => ../databases
	github.com/couchbase/tools-common/environment => ../environment
	github.com/couchbase/tools-common/errors => ../errors
	github.com/couchbase/tools-common/fs => ../fs
	github.com/couchbase/tools-common/functional => ../functional
	github.com/couchbase/tools-common/http => ../http
	github.com/couchbase/tools-common/strings => ../strings
	github.com/couchbase/tools-common/sync => ../sync
	github.com/couchbase/tools-common/testing => ../testing
	github.com/couchbase/tools-common/types => ../types
	github.com/couchbase/tools-common/utils => ../utils
)
