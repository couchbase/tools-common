package connstr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSRVPort(t *testing.T) {
	require.Equal(t, uint16(DefaultHTTPSPort), srvPort("couchbases"))
	require.Equal(t, uint16(DefaultHTTPPort), srvPort("couchbase"))
	require.Equal(t, uint16(DefaultHTTPPort), srvPort("asdf"))
}
