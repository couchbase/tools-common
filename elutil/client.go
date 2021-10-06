package elutil

import (
	"net/http"

	"github.com/couchbase/tools-common/cbrest"
)

const (
	// EndpointPostEvent is the endpoint used to 'POST' events.
	EndpointPostEvent cbrest.Endpoint = "/_event"
)

// Client is a wrapper around the 'cbrest' client which implements the required methods to log events.
type Client struct {
	*cbrest.Client
}

// NewClient creates a new client using the given options, the client may communicate using basic/cert auth depending on
// the auth provider/tls config provided.
func NewClient(options ServiceOptions) (*Client, error) {
	client, err := cbrest.NewClient(cbrest.ClientOptions{
		ConnectionString: options.ConnectionString,
		Provider:         options.Provider,
		TLSConfig:        options.TLSConfig,
		ReqResLogLevel:   options.ReqResLogLevel,
		DisableCCP:       true,
		ThisNodeOnly:     true,
	})

	return &Client{client}, err
}

// PostEvent posts the given encoded event to the local 'ns_server' instance.
func (c *Client) PostEvent(event []byte) error {
	request := &cbrest.Request{
		ContentType:        cbrest.ContentTypeJSON,
		Body:               event,
		Endpoint:           EndpointPostEvent,
		ExpectedStatusCode: http.StatusOK,
		Method:             http.MethodPost,
		Service:            cbrest.ServiceManagement,

		// We check this head of time when using the correct API, however, we should explicitly not retry if the body we
		// 'POST' is too large.
		NoRetryOnStatusCodes: []int{http.StatusRequestEntityTooLarge},
	}

	_, err := c.Execute(request)

	return err
}
