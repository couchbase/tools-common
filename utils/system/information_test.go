package system

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetInformationHonorGOMAXPROCS(t *testing.T) {
	runCPUTest(func() {
		old := runtime.GOMAXPROCS(1)
		defer runtime.GOMAXPROCS(old)

		info := GetInformation(nil)
		require.Equal(t, 1, info.VCPU)
	})
}
