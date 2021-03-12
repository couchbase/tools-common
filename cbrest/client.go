package cbrest

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/couchbase/tools-common/cbvalue"
	"github.com/couchbase/tools-common/connstr"
	"github.com/couchbase/tools-common/envvar"
	"github.com/couchbase/tools-common/log"
	"github.com/couchbase/tools-common/maths"
	"github.com/couchbase/tools-common/netutil"
	"github.com/couchbase/tools-common/slice"
)

// ClientOptions encapsulates the options for creating a new REST client.
type ClientOptions struct {
	ConnectionString string
	Username         string
	Password         string
	UserAgent        string
	TLSConfig        *tls.Config

	// ReqResLogLevel is the level at which to the dispatching and receiving of requests/responses.
	ReqResLogLevel log.Level
}

// Client is a REST client used to retrieve/send information to/from a Couchbase Cluster.
type Client struct {
	client       *http.Client
	authProvider *AuthProvider
	clusterInfo  *cbvalue.ClusterInfo

	pollTimeout    time.Duration
	requestRetries int
	requestTimeout time.Duration

	reqResLogLevel log.Level
}

// NewClient creates a new REST client which will connection to the provided cluster using the given credentials.
func NewClient(options ClientOptions) (*Client, error) {
	clientTimeout, ok := envvar.GetDurationBC("CB_REST_CLIENT_TIMEOUT_SECS")
	if !ok {
		clientTimeout = DefaultClientTimeout
	} else {
		log.Infof("(REST) Set HTTP client timeout to: %s", clientTimeout)
	}

	requestTimeout, ok := envvar.GetDuration("CB_REST_CLIENT_REQUEST_TIMEOUT")
	if !ok {
		requestTimeout = DefaultRequestTimeout
	} else {
		log.Infof("(REST) Set request timeout to: %s", requestTimeout)
	}

	requestRetries, ok := envvar.GetInt("CB_REST_CLIENT_NUM_RETRIES")
	if !ok || requestRetries <= 0 {
		requestRetries = DefaultRequestRetries
	} else {
		log.Infof("(REST) Set number of retries for requests to: %d", requestRetries)
	}

	pollTimeout, ok := envvar.GetDuration("CB_REST_CLIENT_POLL_TIMEOUT")
	if !ok {
		pollTimeout = DefaultPollTimeout
	} else {
		log.Infof("(REST) Set poll timeout to: %s", pollTimeout)
	}

	parsed, err := connstr.Parse(options.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	resolved, err := parsed.Resolve()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve connection string: %w", err)
	}

	authProvider := NewAuthProvider(resolved, options.Username, options.Password, options.UserAgent)

	client := &Client{
		client: &http.Client{
			// NOTE: The HTTP client timeout should be the larger of the two configurable timeouts to avoid one cutting
			// the other short.
			Timeout:   time.Duration(maths.Max(int(requestTimeout), int(clientTimeout))),
			Transport: &http.Transport{TLSClientConfig: options.TLSConfig},
		},
		authProvider:   authProvider,
		pollTimeout:    pollTimeout,
		requestRetries: requestRetries,
		requestTimeout: requestTimeout,
		reqResLogLevel: options.ReqResLogLevel,
	}

	// Attempt to bootstrap the HTTP client, internally the auth provider will return the next available bootstrap host
	// for successive calls until we run out of possible hosts (at which point we exit having failed to bootstrap).
	var (
		errAuthentication *AuthenticationError
		errAuthorization  *AuthorizationError
	)

	for {
		err = client.bootstrap()

		// We've successfully bootstrapped the client
		if err == nil {
			break
		}

		// For security reasons, return immediately if one of the provided nodes is an unknown authority
		var errUnknownAuthority *UnknownAuthorityError
		if errors.As(err, &errUnknownAuthority) {
			return nil, err
		}

		if errors.Is(err, errExhaustedBootstrapHosts) {
			return nil, &BootstrapFailureError{ErrAuthentication: errAuthentication, ErrAuthorization: errAuthorization}
		}

		// If we've hit an authorization/permission error, we will continue trying to bootstrap because this node may no
		// longer be in the cluster, however, we'll slightly modify our possible returned error message to indicate that
		// the user should check their credentials are correct.
		errors.As(err, &errAuthentication)
		errors.As(err, &errAuthorization)

		log.Warnf("(REST) failed to bootstrap client, will retry: %v", err)
	}

	// Get commonly used information about the cluster now to avoid multiple duplicate requests at a later date
	client.clusterInfo, err = client.GetClusterInfo()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster information: %w", err)
	}

	client.logConnectionInfo()

	return client, nil
}

// bootstrap gets the nodes in the cluster from the first address in the connection string and use those to bootstrap
// the auth provider.
func (c *Client) bootstrap() error {
	nodes, err := c.GetNodes()
	if err != nil {
		return fmt.Errorf("failed to get cluster nodes: %w", err)
	}

	c.authProvider.SetNodes(nodes)

	return nil
}

// logConnectionInfo marshals and logs the common cluster information.
func (c *Client) logConnectionInfo() {
	data, err := json.Marshal(c.clusterInfo)
	if err != nil {
		log.Warnf("(REST) Failed to marshal cluster information")
	}

	log.Infof("(REST) Successfully connected to cluster | %s", data)
}

// EnterpriseCluster returns a boolean indicating whether this is an enterprise cluster.
func (c *Client) EnterpriseCluster() bool {
	return c.clusterInfo.Enterprise
}

// ClusterUUID returns the cluster uuid.
func (c *Client) ClusterUUID() string {
	return c.clusterInfo.UUID
}

// ClusterVersion returns the version information extracted from the cluster after bootstrapping.
func (c *Client) ClusterVersion() cbvalue.ClusterVersion {
	return c.clusterInfo.Version
}

// PollTimeout returns the poll timeout used by the current client.
func (c *Client) PollTimeout() time.Duration {
	return c.pollTimeout
}

// RequestTimeout returns the request timeout used by the current client.
func (c *Client) RequestTimeout() time.Duration {
	return c.requestTimeout
}

// RequestRetries returns the number of times a request will be retried for known failure cases.
func (c *Client) RequestRetries() int {
	return c.requestRetries
}

// Nodes returns a copy of the slice of all the nodes in the cluster.
//
// NOTE: This function returns a copy because this is the same data structure the client uses to dispatch REST requests.
func (c *Client) Nodes() Nodes {
	return c.authProvider.nodes.Copy()
}

// TLS returns a boolean indicating whether SSL/TLS is currently enabled.
func (c *Client) TLS() bool {
	return c.authProvider.resolved.UseSSL
}

// AltAddr returns a boolean indicating whether alternate addressing is currently enabled.
func (c *Client) AltAddr() bool {
	return c.authProvider.useAltAddr
}

// Execute the given request to completion reading the entire response body whilst honoring request level
// retries/timeout.
func (c *Client) Execute(request *Request) (*Response, error) {
	// Create a context which allows use to control the timeout for this request over multiple retries
	ctx, cancelFunc := context.WithTimeout(context.Background(), c.requestTimeout)
	defer cancelFunc()

	resp, err := c.Do(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	response := &Response{
		StatusCode: resp.StatusCode,
	}

	response.Body, err = readBody(request, resp)
	if err != nil {
		return response, fmt.Errorf("failed to read response body: %w", err)
	}

	if response.StatusCode == request.ExpectedStatusCode {
		return response, nil
	}

	switch response.StatusCode {
	case http.StatusForbidden:
		type overlay struct {
			Permissions []string `json:"permissions"`
		}

		var data overlay

		// Purposely ignored as some endpoints may not return the permissions or a body at all. In this case we just set
		// the permissions in the AuthorizationError to nil.
		_ = json.NewDecoder(resp.Body).Decode(&data)

		return response, &AuthorizationError{
			method:      request.Method,
			endpoint:    request.Endpoint,
			permissions: data.Permissions,
		}
	case http.StatusUnauthorized:
		return response, &AuthenticationError{method: request.Method, endpoint: request.Endpoint}
	case http.StatusInternalServerError:
		return response, &InternalServerError{method: request.Method, endpoint: request.Endpoint}
	case http.StatusNotFound:
		return response, &EndpointNotFoundError{method: request.Method, endpoint: request.Endpoint}
	}

	return response, &UnexpectedStatusCodeError{
		Status:   response.StatusCode,
		method:   request.Method,
		endpoint: request.Endpoint,
		empty:    resp.ContentLength == 0,
	}
}

// Do converts and executes the provided request returning the raw HTTP response. In general users should prefer to use
// the 'Execute' function which handles closing resources and returns more informative errors.
//
// NOTE: If the returned error is nil, the Response will contain a non-nil Body which the caller is expected to close.
func (c *Client) Do(ctx context.Context, request *Request) (*http.Response, error) {
	var (
		response *http.Response
		codes    []int
		err      error
	)

	for attempt := 0; attempt < c.requestRetries; attempt++ {
		response, err = c.do(ctx, request, attempt+1)
		if err != nil {
			return nil, fmt.Errorf("failed to perform REST request on attempt %d: %w", attempt, err)
		}

		if response.StatusCode == request.ExpectedStatusCode {
			return response, nil
		}

		// We don't log at error level because we expect some requests to fail and be explicitly handled by the caller
		// for example when checking if a bucket exists.
		log.Warnf("(REST) (Attempt %d) (%s) Request to endpoint '%s' failed with status code %d", attempt,
			request.Method, request.Endpoint, response.StatusCode)

		// We've failed with a status code which can't be retried return the response to the caller
		if !(netutil.IsTemporaryFailure(response.StatusCode) ||
			slice.ContainsInt(request.RetryOnStatusCodes, response.StatusCode)) {
			return response, nil
		}

		// We're going to be retrying this request, ensure the response body is closed to avoid resource leaks
		response.Body.Close()

		// Add the status code to the list of retried status code; this information is returned to the called if we
		// exhaust the maximum number of retries.
		codes = append(codes, response.StatusCode)

		log.Warnf("(REST) (Attempt %d) (%s) Retrying request to endpoint '%s' which failed with status code %d",
			attempt, request.Method, request.Endpoint, response.StatusCode)
	}

	return nil, &RetriesExhaustedError{retries: c.requestRetries, codes: codes}
}

// do is a convenience which prepares then performs the provided request.
func (c *Client) do(ctx context.Context, request *Request, attempt int) (*http.Response, error) {
	prep, err := c.prepare(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare request: %w", err)
	}

	resp, err := c.perform(prep, attempt)
	if err != nil {
		return nil, fmt.Errorf("failed to perform request: %w", err)
	}

	return resp, nil
}

// prepare converts the request into a raw HTTP request which can be dispatched to the cluster. Uses the same context
// meaning the request timeout is not reset by retries.
//
// NOTE: Recalculates the host address/port meaning in the future we can re-bootstrap the client using CCCP to make the
// REST client more robust.
func (c *Client) prepare(ctx context.Context, request *Request) (*http.Request, error) {
	// Get the fully qualified address to the node that we are sending this request to
	host, err := c.authProvider.GetServiceHost(request.Service)
	if err != nil {
		return nil, fmt.Errorf("failed to get host for service '%s': %w", request.Service, err)
	}

	req, err := http.NewRequestWithContext(ctx, string(request.Method), host+string(request.Endpoint),
		bytes.NewReader(request.Body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// If we received one or more non-nil query parameters ensure that they will be postfixed to the request URL.
	if len(request.QueryParameters) != 0 {
		req.URL.RawQuery = request.QueryParameters.Encode()
	}

	// Using 'Set' overwrites an existing values set in the header, set these values first to that the settings below
	// take precedence.
	for key, value := range request.Header {
		req.Header.Set(key, value)
	}

	// Use the auth provider to populate the credentials
	req.SetBasicAuth(c.authProvider.GetCredentials(host))

	// Set the 'User-Agent' so that we can trace how these requests are handled by the cluster
	req.Header.Set("User-Agent", c.authProvider.GetUserAgent())

	// Set the content type for the request body. Note that we don't default to a value e.g. if must be set for every
	// request otherwise the string zero value will be used.
	req.Header.Set("Content-Type", string(request.ContentType))

	return req, nil
}

// perform synchronously executes the provided request returning the response and any error that occurred during the
// process.
func (c *Client) perform(req *http.Request, attempt int) (*http.Response, error) {
	log.Logf(c.reqResLogLevel, "(REST) (Attempt %d) (%s) Dispatching request to '%s'", attempt, req.Method, req.URL)

	resp, err := c.client.Do(req)
	if err == nil {
		log.Logf(c.reqResLogLevel, "(REST) (Attempt %d) (%s) (%d) Received response from '%s'", attempt, req.Method,
			resp.StatusCode, req.URL)

		return resp, nil
	}

	log.Errorf("(REST) (Attempt %d) (%s) Failed to perform request to '%s': %s", attempt, req.Method, req.URL, err)

	// If we received and unknown authority error, wrap it with our informative error explaining the alternatives
	// available to the user.
	var unknownAuth x509.UnknownAuthorityError
	if errors.As(err, &unknownAuth) {
		return nil, &UnknownAuthorityError{inner: err}
	}

	// If we receive an EOF error, wrap it with a useful error message containing the method/endpoint
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, &SocketClosedInFlightError{method: req.Method, endpoint: req.URL.Path}
	}

	return nil, err
}

// GetNodes gets a list of all the nodes (and the services they run) in the cluster. Provided to the auth provider
// during bootstrapping to send future requests to nodes running specified services.
func (c *Client) GetNodes() (Nodes, error) {
	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           EndpointNodesServices,
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	response, err := c.Execute(request)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	var nodes Nodes

	err = json.Unmarshal(response.Body, &nodes)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	fallback := c.authProvider.GetFallbackHost()

	for _, node := range nodes {
		// We should only populate the hostname if we're not bootstrapping using the alternate hostname; if we populate
		// the internal hostname with the external hostname, then the alternate addressing mode won't be triggered. This
		// could lead to a situtation where we're dispatching requests to the alternate hostname, but using the internal
		// ports.
		if node.Hostname == "" &&
			!(node.AlternateAddresses.External != nil && fallback == node.AlternateAddresses.External.Hostname) {
			node.Hostname = fallback
		}

		// We "reconstruct" ipv6 addresses by surrounding them with brackets
		node.Hostname = netutil.ReconstructIPV6(node.Hostname)

		// We do the same for possible ipv6 alternate addresses
		if node.AlternateAddresses.External != nil {
			node.AlternateAddresses.External.Hostname =
				netutil.ReconstructIPV6(node.AlternateAddresses.External.Hostname)
		}
	}

	return nodes, nil
}

// GetServiceHost retrieves the address for a single node in the cluster which is running the provided service.
func (c *Client) GetServiceHost(service Service) (string, error) {
	return c.authProvider.GetServiceHost(service)
}

// GetAllServiceHosts retrieves a list of all the nodes in the cluster that are running the provided service.
func (c *Client) GetAllServiceHosts(service Service) ([]string, error) {
	return c.authProvider.GetAllServiceHosts(service)
}

// GetClusterInfo gets commonly used information about the cluster; this includes the uuid and version.
func (c *Client) GetClusterInfo() (*cbvalue.ClusterInfo, error) {
	enterprise, uuid, err := c.GetClusterMetaData()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster metadata: %w", err)
	}

	version, err := c.GetClusterVersion()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster version: %w", err)
	}

	return &cbvalue.ClusterInfo{Enterprise: enterprise, UUID: uuid, Version: version}, nil
}

// GetClusterMetaData extracts some common metadata from the cluster. Returns a boolean indicating if this is an
// enterprise cluster, and the cluster uuid.
func (c *Client) GetClusterMetaData() (bool, string, error) {
	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           EndpointPools,
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	response, err := c.Execute(request)
	if err != nil {
		return false, "", fmt.Errorf("failed to execute request: %w", err)
	}

	type overlay struct {
		Enterprise bool   `json:"isEnterprise"`
		UUID       string `json:"uuid"`
	}

	var decoded *overlay

	err = json.Unmarshal(response.Body, &decoded)
	if err != nil {
		// We will fail to unmarshal the response from the node if it's uninitialized, this is because the "uuid" field
		// will be an empty array, instead of a string; if this is the case, return a clearer error message.
		if bytes.Contains(response.Body, []byte(`"uuid":[]`)) {
			return false, "", ErrNodeUninitialized
		}

		return false, "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return decoded.Enterprise, decoded.UUID, nil
}

// GetClusterVersion extracts version information from the cluster nodes.
func (c *Client) GetClusterVersion() (cbvalue.ClusterVersion, error) {
	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           EndpointPoolsDefault,
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	response, err := c.Execute(request)
	if err != nil {
		return cbvalue.ClusterVersion{}, fmt.Errorf("failed to execute request: %w", err)
	}

	type overlay struct {
		Nodes []struct {
			Version string `json:"version"`
		} `json:"nodes"`
	}

	var decoded *overlay

	err = json.Unmarshal(response.Body, &decoded)
	if err != nil {
		return cbvalue.ClusterVersion{}, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	clusterVersion := cbvalue.ClusterVersion{
		MinVersion: cbvalue.Version(strings.Split(decoded.Nodes[0].Version, "-")[0]),
	}

	for _, node := range decoded.Nodes {
		nodeVersion := cbvalue.Version(strings.Split(node.Version, "-")[0])
		if clusterVersion.MinVersion == nodeVersion {
			continue
		}

		if nodeVersion < clusterVersion.MinVersion {
			clusterVersion.MinVersion = nodeVersion
		}

		clusterVersion.Mixed = true
	}

	return clusterVersion, nil
}
