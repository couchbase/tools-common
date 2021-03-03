package cbrest

// Service represents a service which can be running on a Couchbase node.
type Service string

const (
	// ServiceManagement represents 'ns_server' e.g. port 8091/18091.
	ServiceManagement Service = "Management"

	// ServiceAnalytics represents the Analytics Service.
	ServiceAnalytics Service = "Analytics"

	// ServiceData represents the KV/Data Service.
	ServiceData Service = "Data"

	// ServiceEventing represents the cluster level Eventing Service.
	ServiceEventing Service = "Eventing"

	// ServiceGSI represents the Indexing Service.
	ServiceGSI Service = "Indexing"

	// ServiceQuery represents the Query Service e.g. nodes running N1QL.
	ServiceQuery Service = "Query"

	// ServiceSearch represents the Search/Full Text Search Service.
	ServiceSearch Service = "Search"

	// ServiceViews represents hosts accepting requests for Views.
	ServiceViews Service = "Views"

	// ServiceBackup represents hosts accepting requests for the Backup Service.
	ServiceBackup Service = "Backup"
)
