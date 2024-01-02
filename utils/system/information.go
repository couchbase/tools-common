package system

import (
	"fmt"
	"log/slog"
	"os"
	"runtime"

	"github.com/couchbase/tools-common/strings/format"
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
func GetInformation(logger *slog.Logger) Information {
	if logger == nil {
		logger = slog.Default()
	}

	def := func(s string) string {
		if s == "" {
			return "unavailable"
		}

		return s
	}

	hostname, err := os.Hostname()
	if err != nil {
		logger.Error("failed to system hostname", "error", err)
	}

	version, err := Version()
	if err != nil {
		logger.Error("failed to get system version", "error", err)
	}

	memory, err := TotalMemory()
	if err != nil {
		logger.Error("failed to get system total memory", "error", err)
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
