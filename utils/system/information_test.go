package system

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/tools-common/core/log"
)

func TestGetInformationHonorGOMAXPROCS(t *testing.T) {
	runCPUTest(func() {
		old := runtime.GOMAXPROCS(1)
		defer runtime.GOMAXPROCS(old)

		info := GetInformation(log.StdoutLogger{})
		require.Equal(t, 1, info.VCPU)
	})
}
