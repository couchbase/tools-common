package cbrest

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/tools-common/aprov"
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
	Provider         aprov.Provider
	TLSConfig        *tls.Config

	// DisableCCP stops the client from periodically updating the cluster config. This should only be used if you know
	// what you're doing and you're only using a client for a short period of time, otherwise, it's possible for some
	// client functions to return stale data/attempt to address missing nodes.
	DisableCCP bool

	// ThisNodeOnly is only meant to be used if you only want to exclusively communicate with the given host. It should
	// only be used for short lived clients, ideally doing a couple of requests at most.
	ThisNodeOnly bool

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

	reqResLogLevel log.Level

	wg         sync.WaitGroup
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// NewClient creates a new REST client which will connection to the provided cluster using the given credentials.
//
// NOTE: The returned client may (depending on the provided options) acquire resources which must be cleaned up using
// the clients 'Close' function. For example, the 'Close' function must be called to cleanup the cluster config polling
// goroutine.
func NewClient(options ClientOptions) (*Client, error) {
	clientTimeout, ok := envvar.GetDurationBC("CB_REST_CLIENT_TIMEOUT_SECS")
	if !ok {
		clientTimeout = DefaultClientTimeout
	} else {
		log.Infof("(REST) Set HTTP client timeout to: %s", clientTimeout)
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

	authProvider := NewAuthProvider(resolved, options.Provider)

	client := &Client{
		client: &http.Client{
			Timeout: clientTimeout,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:   true,
				IdleConnTimeout:     90 * time.Second,
				MaxIdleConns:        100,
				Proxy:               http.ProxyFromEnvironment,
				TLSClientConfig:     options.TLSConfig,
				TLSHandshakeTimeout: 10 * time.Second,
			},
		},
		authProvider:   authProvider,
		pollTimeout:    pollTimeout,
		requestRetries: requestRetries,
		reqResLogLevel: options.ReqResLogLevel,
	}

	err = client.bootstrap(options.ThisNodeOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to bootstrap client: %w", err)
	}

	// Get commonly used information about the cluster now to avoid multiple duplicate requests at a later date
	client.clusterInfo, err = client.GetClusterInfo()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster information: %w", err)
	}

	client.logConnectionInfo()

	// Cluster config polling must not begin until we've fetched the cluster information, this is because it relies on
	// having the cluster uuid to determine whether it's safe to use a given cluster config.
	if !(options.ThisNodeOnly || options.DisableCCP) {
		client.beginCCP()
	}

	return client, nil
}

// bootstrap attempts to bootstrap the client using the hosts from the given collection string provided by the user. The
// optional argument 'thisNodeOnly' may be supplied to force all communication with the bootstrap node.
func (c *Client) bootstrap(thisNodeOnly bool) error {
	// Attempt to bootstrap the HTTP client, internally the auth provider will return the next available bootstrap host
	// for successive calls until we run out of possible hosts (at which point we exit having failed to bootstrap).
	var (
		hostFunc          = c.authProvider.bootstrapHostFunc()
		errAuthentication *AuthenticationError
		errAuthorization  *AuthorizationError
	)

	for {
		host := hostFunc()

		// If this call returned an empty hostname then we've tried all the available hostnames and we've failed to
		// bootstrap against any of them.
		if host == "" {
			return &BootstrapFailureError{ErrAuthentication: errAuthentication, ErrAuthorization: errAuthorization}
		}

		err := c.updateCCFromHost(host, thisNodeOnly)

		// We've successfully bootstrapped the client
		if err == nil {
			break
		}

		var (
			errUnknownAuthority *UnknownAuthorityError
			errUnknownX509Error *UnknownX509Error
		)

		// For security reasons, return immediately if the user is connecting using TLS and we've received an x509 error
		if errors.As(err, &errUnknownAuthority) || errors.As(err, &errUnknownX509Error) {
			return err
		}

		// If we've hit an authorization/permission error, we will continue trying to bootstrap because this node may no
		// longer be in the cluster, however, we'll slightly modify our possible returned error message to indicate that
		// the user should check their credentials are correct.
		errors.As(err, &errAuthentication)
		errors.As(err, &errAuthorization)

		log.Warnf("(REST) failed to bootstrap client, will retry: %v", err)
	}

	return nil
}

// beginCCP is a utility function which sets up and begins the cluster config polling goroutine.
func (c *Client) beginCCP() {
	// Ensure we add to the wait group before spinning up the polling goroutine
	c.wg.Add(1)

	// Allow the proper cleanup of the goroutine when the user calls 'Close'
	c.ctx, c.cancelFunc = context.WithCancel(context.Background())

	// Spin up a goroutine which will periodically update the clients cluster config the client allowing it to
	// correctly handle dynamic changes to the target cluster; this includes proper handling/detection of
	// adding/removing nodes.
	go c.pollCC()
}

// pollCC loops until cancelled updating the clients cluster config. This goroutine will be cleaned up after a call to
// 'Close'.
func (c *Client) pollCC() {
	defer c.wg.Done()

	for {
		c.authProvider.manager.WaitUntilExpired(c.ctx)

		if c.ctx.Err() != nil {
			return
		}

		if err := c.updateCC(); err != nil {
			log.Warnf("(REST) Failed to update cluster config, will retry: %v", err)
		}
	}
}

// updateCC attempts to update the cluster config using each of the known nodes in the cluster.
//
// NOTE: It's possible for this to completely fail if we were unable find a valid config from any node in the cluster.
func (c *Client) updateCC() error {
	config := c.authProvider.manager.GetClusterConfig()

	// Always try to use the node that we initially bootstrapped against first, failing that we'll continue trying the
	// other nodes in the cluster.
	sort.Slice(config.Nodes, func(i, _ int) bool { return config.Nodes[i].BootstrapNode })

	for _, node := range config.Nodes {
		err := c.updateCCFromNode(node)

		// We've successfully updated the config, don't continue retrying against other nodes
		if err == nil {
			return nil
		}

		// NOTE: This function is slightly different to the initial bootstrapping, in the event that we receive an
		// 'UnknownAuthorityError' we continue using the next node; we do this because that node may have been removed
		// from the cluster.

		log.Warnf("(REST) (CCP) Failed to update config using host '%s': %v", node.Hostname, err)
	}

	return ErrExhaustedClusterNodes
}

// updateCCFromNode will attempt to update the client's 'AuthProvider' using the provided node.
func (c *Client) updateCCFromNode(node *Node) error {
	host, _ := node.GetQualifiedHostname(ServiceManagement, c.authProvider.resolved.UseSSL, c.authProvider.useAltAddr)
	if host == "" {
		return fmt.Errorf("node is not running the management service")
	}

	valid, err := c.validHost(host)
	if err != nil {
		return fmt.Errorf("failed to check if node is valid: %w", err)
	}

	if !valid {
		return fmt.Errorf("node is a member of a different cluster")
	}

	return c.updateCCFromHost(host, false)
}

// updateCCFromHost will attempt to update the clients cluster config using the provided host.
func (c *Client) updateCCFromHost(host string, thisNodeOnly bool) error {
	body, err := c.get(host, EndpointNodesServices)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}

	// This shouldn't really fail since we should be constructing valid hosts in the auth provider
	parsed, err := url.Parse(host)
	if err != nil {
		return fmt.Errorf("failed to parse host '%s': %w", host, err)
	}

	// We must extract the raw hostname (no port) so that it can be used as the fallback host
	host = parsed.Hostname()

	config, err := c.unmarshalCC(host, body)
	if err != nil {
		return fmt.Errorf("failed to unmarshal cluster config: %w", err)
	}

	if thisNodeOnly {
		config.FilterOtherNodes()
	}

	return c.authProvider.SetClusterConfig(host, config)
}

// validHost returns a boolean indicating whether we should use the cluster config from the provided host. This should
// help to avoid the case where we try to get a cluster config from a node which has joined another cluster.
func (c *Client) validHost(host string) (bool, error) {
	if c.clusterInfo == nil {
		return true, nil
	}

	body, err := c.get(host, EndpointPools)
	if err != nil {
		return false, fmt.Errorf("failed to execute request: %w", err)
	}

	type overlay struct {
		UUID string `json:"uuid"`
	}

	var decoded *overlay

	err = json.Unmarshal(body, &decoded)
	if err == nil {
		return decoded.UUID == c.clusterInfo.UUID, nil
	}

	// We will fail to unmarshal the response from the node if it's uninitialized, this is because the "uuid" field will
	// be an empty array, instead of a string; if this is the case, return false because we shouldn't use the cluster
	// config from this node.
	if bytes.Contains(body, []byte(`"uuid":[]`)) {
		return false, nil
	}

	return false, fmt.Errorf("failed to unmarshal cluster config: %w", err)
}

// get is similar to the public 'Execute' function, however, it is meant only to be used internally is less flexible and
// doesn't support automatic retries.
func (c *Client) get(host string, endpoint Endpoint) ([]byte, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), defaultInternalRequestTimeout)
	defer cancelFunc()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, host+string(endpoint), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	setAuthHeaders(host, c.authProvider, req)

	resp, err := c.perform(req, log.LevelDebug, 1, 0)
	if err != nil {
		return nil, handleRequestError(req, err) // Purposefully not wrapped
	}
	defer resp.Body.Close()

	body, err := readBody(http.MethodGet, endpoint, resp.Body, resp.ContentLength)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, handleResponseError(http.MethodGet, endpoint, resp.StatusCode, body)
	}

	return body, nil
}

// unmarshalCC is a utility function which handles unmarshalling the cluster config response whilst cleaning
// it up so that it can be used by the client's 'AuthProvider'.
func (c *Client) unmarshalCC(host string, body []byte) (*ClusterConfig, error) {
	var config *ClusterConfig

	err := json.Unmarshal(body, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	for _, node := range config.Nodes {
		// We should only populate the hostname if we're not bootstrapping using the alternate hostname; if we populate
		// the internal hostname with the external hostname, then the alternate addressing mode won't be triggered. This
		// could lead to a situtation where we're dispatching requests to the alternate hostname, but using the internal
		// ports.
		if node.Hostname == "" &&
			!(node.AlternateAddresses.External != nil && host == node.AlternateAddresses.External.Hostname) {
			node.Hostname = host
		}

		// We "reconstruct" ipv6 addresses by surrounding them with brackets
		node.Hostname = netutil.ReconstructIPV6(node.Hostname)

		// We do the same for possible ipv6 alternate addresses
		if node.AlternateAddresses.External != nil {
			node.AlternateAddresses.External.Hostname =
				netutil.ReconstructIPV6(node.AlternateAddresses.External.Hostname)
		}
	}

	return config, nil
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
//
// NOTE: This function may return stale data, for the most up-to-date information, use 'GetClusterMetaData'.
func (c *Client) EnterpriseCluster() bool {
	return c.clusterInfo.Enterprise
}

// ClusterUUID returns the cluster uuid.
//
// NOTE: This function may return stale data, for the most up-to-date information, use 'GetClusterMetaData'
func (c *Client) ClusterUUID() string {
	return c.clusterInfo.UUID
}

// ClusterVersion returns the version information extracted from the cluster after bootstrapping.
//
// NOTE: This function may return stale data, for the most up-to-date information, use 'GetClusterVersion'.
func (c *Client) ClusterVersion() cbvalue.ClusterVersion {
	return c.clusterInfo.Version
}

// MaxVBuckets returns the maximum number of vBuckets on the target cluster.
//
// NOTE: This function may return stale data, for the most up-to-date information, use 'GetMaxVBuckets'.
func (c *Client) MaxVBuckets() uint16 {
	return c.clusterInfo.MaxVBuckets
}

// UniformVBuckets returns a boolean indicating whether all the buckets on the cluster have the same amount of vBuckets.
//
// NOTE: This function may return stale data, for the most up-to-date information, use 'GetMaxVBuckets'.
func (c *Client) UniformVBuckets() bool {
	return c.clusterInfo.UniformVBuckets
}

// PollTimeout returns the poll timeout used by the current client.
func (c *Client) PollTimeout() time.Duration {
	return c.pollTimeout
}

// RequestRetries returns the number of times a request will be retried for known failure cases.
func (c *Client) RequestRetries() int {
	return c.requestRetries
}

// Nodes returns a copy of the slice of all the nodes in the cluster.
//
// NOTE: This function returns a copy because this is the same data structure the client uses to dispatch REST requests.
func (c *Client) Nodes() Nodes {
	if config := c.authProvider.manager.GetClusterConfig(); config != nil {
		return config.Nodes
	}

	return make(Nodes, 0)
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
	resp, err := c.Do(context.Background(), request)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	response := &Response{
		StatusCode: resp.StatusCode,
	}

	response.Body, err = readBody(request.Method, request.Endpoint, resp.Body, resp.ContentLength)
	if err != nil {
		return response, fmt.Errorf("failed to read response body: %w", err)
	}

	if response.StatusCode == request.ExpectedStatusCode {
		return response, nil
	}

	return response, handleResponseError(request.Method, request.Endpoint, response.StatusCode, response.Body)
}

// Do converts and executes the provided request returning the raw HTTP response. In general users should prefer to use
// the 'Execute' function which handles closing resources and returns more informative errors.
//
// NOTE: If the returned error is nil, the Response will contain a non-nil Body which the caller is expected to close.
func (c *Client) Do(ctx context.Context, request *Request) (*http.Response, error) {
	var (
		response *http.Response
		err      error
	)

	for attempt := 1; attempt <= c.requestRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		response, err = c.do(ctx, request, attempt)
		if err != nil {
			if c.shouldRetry(ctx, err) {
				log.Warnf("(REST) (Attempt %d) (%s) Retrying request to endpoint '%s' which failed due to error: %v",
					attempt, request.Method, request.Endpoint, err)
				continue
			}

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
		if !c.shouldRetryWithReqRes(ctx, request, response) {
			return response, nil
		}

		// We're going to be retrying this request, ensure the response body is closed to avoid resource leaks, we don't
		// close the response body for the final attempt since we'll include the body in the returned error.
		if attempt != c.requestRetries {
			response.Body.Close()
		}

		log.Warnf("(REST) (Attempt %d) (%s) Retrying request to endpoint '%s' which failed with status code %d",
			attempt, request.Method, request.Endpoint, response.StatusCode)
	}

	// If we have a non-nil response try an convert the error into one of our more informative errors
	if err == nil && response != nil {
		// Attempt to read the response body, this will help improve the returned error message
		defer response.Body.Close()
		body, _ := readBody(request.Method, request.Endpoint, response.Body, response.ContentLength)

		err = handleResponseError(request.Method, request.Endpoint, response.StatusCode, body)
	}

	return nil, &RetriesExhaustedError{retries: c.requestRetries, err: err}
}

// shouldRetry returns a boolean indicating whether the given error is retryable.
func (c *Client) shouldRetry(ctx context.Context, err error) bool {
	if !shouldRetry(err) {
		return false
	}

	// We always update the cluster config after a failed request, since some connection failures may be due to an
	// attempt to address a node which is no longer a member of the cluster or even running Couchbase Server. For
	// example, the 'connection refused' error.
	c.waitUntilUpdated(ctx)

	return true
}

// shouldRetryWithReqRes returns a boolean indicating whether the given request is retryable.
//
// NOTE: When CCP is enabled, this function may block until the client has the latest available cluster config.
func (c *Client) shouldRetryWithReqRes(ctx context.Context, request *Request, response *http.Response) bool {
	// The user has explicitly stated that they don't want this status code retried, don't retry
	if slice.ContainsInt(request.NoRetryOnStatusCodes, response.StatusCode) {
		return false
	}

	updateCC := slice.ContainsInt([]int{http.StatusUnauthorized}, response.StatusCode)

	// This could be a failure which is retryable without requiring the cluster config to be updated
	if !updateCC {
		return netutil.IsTemporaryFailure(response.StatusCode) ||
			slice.ContainsInt(request.RetryOnStatusCodes, response.StatusCode)
	}

	c.waitUntilUpdated(ctx)

	return true
}

// waitUntilUpdated blocks the calling goroutine until the cluster config has been updated.
func (c *Client) waitUntilUpdated(ctx context.Context) {
	// If we've got a CCP poller running, we can just wake it up and wait for the update to complete; this is more
	// efficient because we can have multiple requests waiting for the CCP goroutine to update the cluster config
	// at once.
	if !(c.ctx == nil || c.cancelFunc == nil) {
		c.authProvider.manager.WaitUntilUpdated(ctx)
		return
	}

	// Otherwise we're going to have to manually update the config; this isn't ideal because it means more than one
	// failed request may be updating the config concurrently. This should be handled correctly but may result in
	// poorer performance/wasted time.
	err := c.updateCC()
	if err != nil {
		log.Warnf("(REST) Failed to update cluster config, will retry: %v", err)
	}
}

// do is a convenience which prepares then performs the provided request.
func (c *Client) do(ctx context.Context, request *Request, attempt int) (*http.Response, error) {
	prep, err := c.prepare(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare request: %w", err)
	}

	resp, err := c.perform(prep, c.reqResLogLevel, attempt, request.Timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to perform request: %w", err)
	}

	return resp, nil
}

// prepare converts the request into a raw HTTP request which can be dispatched to the cluster. Uses the same context
// meaning the request timeout is not reset by retries.
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

	setAuthHeaders(host, c.authProvider, req)

	// Set the content type for the request body. Note that we don't default to a value e.g. if must be set for every
	// request otherwise the string zero value will be used.
	req.Header.Set("Content-Type", string(request.ContentType))

	return req, nil
}

// perform synchronously executes the provided request returning the response and any error that occurred during the
// process.
func (c *Client) perform(req *http.Request, level log.Level, attempt int,
	customTimeout time.Duration) (*http.Response, error) {
	log.Logf(level, "(REST) (Attempt %d) (%s) Dispatching request to '%s'", attempt, req.Method, req.URL)

	client := c.client
	// We only use the custom timeout if it is bigger than the client one. This is so that it can be overridden via
	// environmental variables.
	if customTimeout > 0 && customTimeout > client.Timeout {
		client = &http.Client{
			Timeout:   customTimeout,
			Transport: client.Transport,
		}
	}

	resp, err := client.Do(req)
	if err == nil {
		log.Logf(level, "(REST) (Attempt %d) (%s) (%d) Received response from '%s'", attempt, req.Method,
			resp.StatusCode, req.URL)

		return resp, nil
	}

	log.Errorf("(REST) (Attempt %d) (%s) Failed to perform request to '%s': %s", attempt, req.Method, req.URL, err)

	return nil, handleRequestError(req, err)
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

	maxVBuckets, uniformVBuckets, err := c.GetMaxVBuckets()
	if err != nil {
		return nil, fmt.Errorf("failed to get max number of vBuckets: %w", err)
	}

	return &cbvalue.ClusterInfo{
		Enterprise:      enterprise,
		UUID:            uuid,
		Version:         version,
		MaxVBuckets:     maxVBuckets,
		UniformVBuckets: uniformVBuckets,
	}, nil
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

// GetMaxVBuckets uses the bucket vBucket maps to determine the maximum number of vBuckets on the target cluster.
func (c *Client) GetMaxVBuckets() (uint16, bool, error) {
	request := &Request{
		ContentType:        ContentTypeURLEncoded,
		Endpoint:           EndpointBuckets,
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodGet,
		Service:            ServiceManagement,
	}

	response, err := c.Execute(request)
	if err != nil {
		return 0, false, fmt.Errorf("failed to execute request: %w", err)
	}

	type overlay struct {
		VBucketServerMap struct {
			VBucketMap [][2]int `json:"vBucketMap"`
		} `json:"VBucketServerMap"`
	}

	var decoded []*overlay

	err = json.Unmarshal(response.Body, &decoded)
	if err != nil {
		return 0, false, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if len(decoded) == 0 {
		return 0, true, nil
	}

	var (
		uniform = true
		max     = len(decoded[0].VBucketServerMap.VBucketMap)
	)

	for _, bucket := range decoded {
		num := len(bucket.VBucketServerMap.VBucketMap)
		uniform = uniform && max == num
		max = maths.Max(max, num)
	}

	return uint16(max), uniform, nil
}

// Close releases any resources that are actively being consumed/used by the client.
func (c *Client) Close() {
	if c.ctx == nil || c.cancelFunc == nil {
		return
	}

	c.cancelFunc()
	c.wg.Wait()

	c.ctx = nil
	c.cancelFunc = nil
}
