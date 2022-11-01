package system

import (
	"runtime"
	"testing"

	"github.com/couchbase/tools-common/log"
	"github.com/stretchr/testify/require"
)

func TestGetInformationHonorGOMAXPROCS(t *testing.T) {
	runCPUTest(func() {
		old := runtime.GOMAXPROCS(1)
		defer runtime.GOMAXPROCS(old)

		info := GetInformation(log.StdoutLogger{})
		require.Equal(t, 1, info.VCPU)
	})
}
