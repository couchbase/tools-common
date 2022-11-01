package system

import (
	"fmt"
	"os"
	"runtime"

	"github.com/couchbase/tools-common/format"
	"github.com/couchbase/tools-common/log"
)

// Information represents useful system information which is logged by 'cbbackupmgr' at the beginning of each command.
type Information struct {
	Hostname string
	OS       string
	Version  string
	Arch     string
	VCPU     int
	Memory   uint64
}

// String implements the 'Stringer' interface note changes to this format should be considered carefully as to note
// break backwards compatibility.
func (i Information) String() string {
	return fmt.Sprintf("Hostname: %s OS: %s Version: %s Arch: %s vCPU: %d Memory: %d (%s)",
		i.Hostname, i.OS, i.Version, i.Arch, i.VCPU, i.Memory, format.Bytes(i.Memory))
}

// GetInformation fetches and returns common system information in a platform agnostic fashion.
//
// NOTE: On supported platforms, the returned information may not be that of the host system but of any limits applied.
func GetInformation(logger log.Logger) Information {
	wrappedLogger := log.NewWrappedLogger(logger)
	def := func(s string) string {
		if s == "" {
			return "unavailable"
		}

		return s
	}

	hostname, err := os.Hostname()
	if err != nil {
		wrappedLogger.Errorf("failed to system hostname: %v", err)
	}

	version, err := Version()
	if err != nil {
		wrappedLogger.Errorf("failed to get system version: %v", err)
	}

	memory, err := TotalMemory()
	if err != nil {
		wrappedLogger.Errorf("failed to get system total memory: %v", err)
	}

	return Information{
		Hostname: def(hostname),
		OS:       runtime.GOOS,
		Version:  def(version),
		Arch:     runtime.GOARCH,
		VCPU:     NumCPU(),
		Memory:   memory,
	}
}
