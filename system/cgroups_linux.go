package system

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// cGroupVersion is the implementation of cgroups we've found.
type cGroupVersion int

const (
	cGroupVersion1 cGroupVersion = iota
	cGroupVersion2
)

const defaultCPUPeriod uint64 = 100000

// cGroupInfo  holds the information parsed from /proc/<pid>/cgroup.
type cGroupInfo struct {
	mountPoints map[string]string
	version     cGroupVersion
}

// getMountPoint - find the mount point of controller. For cgroups v1 each controller can have a difference mount point,
// but for v2 they should all be the same so we look for the default mount
func (i *cGroupInfo) getMountPoint(controller string) (string, error) {
	var (
		path string
		ok   bool
	)

	switch i.version {
	case cGroupVersion1:
		path, ok = i.mountPoints[controller]
	case cGroupVersion2:
		path, ok = i.mountPoints["default"]
	default:
		return "", fmt.Errorf("unknown cgroup version %d", i.version)
	}

	if !ok {
		return "", fmt.Errorf("could not find mount point for %s", controller)
	}

	return path, nil
}

// readCGroupFile  reads from r a file in the format of /proc/<id>/cgroup and populates a cGroupInfo with the mount
// points of different controllers.
func readCGroupFile(r io.Reader) (*cGroupInfo, error) {
	var (
		reader      = bufio.NewReader(r)
		mountPoints = make(map[string]string)
		line        string
		err         error
	)

	// read a file in the /proc/self/cgroup format, eg for v1:
	// 8:memory:/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476
	// and:
	// 0::/
	// for v2

	for line, err = reader.ReadString('\n'); err == nil; line, err = reader.ReadString('\n') {
		split := strings.Split(strings.TrimSpace(line), ":")
		if len(split) != 3 {
			return nil, fmt.Errorf("invalid cgroup file, wrong number of separators (%d not 3)", len(split))
		}

		if split[2] == "" {
			return nil, fmt.Errorf("invalid cgroup file, no mount point")
		}

		if split[1] == "" {
			// cgroup file with no middle value means version 2 with a single mount point, unless we've already had a
			// valid line
			if len(mountPoints) != 0 {
				continue
			}

			return &cGroupInfo{version: cGroupVersion2, mountPoints: map[string]string{"default": split[2]}}, nil
		}

		mountPoints[split[1]] = split[2]
	}

	if !errors.Is(err, io.EOF) || len(mountPoints) == 0 {
		return nil, fmt.Errorf("could not read a line from cgroup file: %w", err)
	}

	return &cGroupInfo{version: cGroupVersion1, mountPoints: mountPoints}, nil
}

var errNoLimitSpecified = errors.New("no cgroup limit specified")

// readMountInfo - will look in r for mountPoint with filesystem type fs. When there are multiple results it will filter
// to those lines whose filesystem options has "contains" as a substring. Returns where mountPoint is mounted.
func readMountInfo(r io.Reader, mountPoint, fs, contains string) (string, error) {
	type mountInfo struct {
		point   string
		options []string
	}

	var (
		err          error
		line         string
		matchingInfo []mountInfo
		reader       = bufio.NewReader(r)
	)

	// Read lines in the following format:
	// 36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue
	// (1)(2)(3)   (4)   (5)      (6)      (7)   (8) (9)   (10)         (11)
	//
	// (1) mount ID:  unique identifier of the mount (may be reused after umount)
	// (2) parent ID:  ID of parent (or of self for the top of the mount tree)
	// (3) major:minor:  value of st_dev for files on filesystem
	// (4) root:  root of the mount within the filesystem
	// (5) mount point:  mount point relative to the process's root
	// (6) mount options:  per mount options
	// (7) optional fields:  zero or more fields of the form "tag[:value]"
	// (8) separator:  marks the end of the optional fields
	// (9) filesystem type:  name of filesystem of the form "type[.subtype]"
	// (10) mount source:  filesystem specific information or "none"
	// (11) super options:  per super block options
	// from https://www.kernel.org/doc/Documentation/filesystems/proc.txt

	for line, err = reader.ReadString('\n'); err == nil; line, err = reader.ReadString('\n') {
		// 7 takes us up to just before the separator (8)
		split := strings.SplitN(strings.TrimSpace(line), " ", 7)
		if len(split) < 7 {
			return "", fmt.Errorf("invalid mountinfo file, less than seven fields")
		}

		separatorSplit := strings.SplitN(split[6], "-", 2)
		if len(separatorSplit) < 2 {
			return "", fmt.Errorf("invalid mountinfo file, no separator")
		}

		postSeparator := strings.Split(strings.TrimSpace(separatorSplit[1]), " ")
		if len(postSeparator) < 2 {
			return "", fmt.Errorf("invalid mountinfo file, no space after separator")
		}

		if split[3] != mountPoint {
			continue
		}

		if postSeparator[0] == fs {
			matchingInfo = append(matchingInfo, mountInfo{point: split[4], options: postSeparator})
		}
	}

	if len(matchingInfo) == 0 {
		return "", errNoLimitSpecified
	}

	if len(matchingInfo) == 1 {
		return matchingInfo[0].point, nil
	}

	for _, info := range matchingInfo {
		for _, option := range info.options {
			if option == contains || option == "rw,"+contains {
				return info.point, nil
			}
		}
	}

	return "", errNoLimitSpecified
}

// getCGroupMemoryLimitFromFile reads the cgroup memory limit using the correct file for version.
func getCGroupMemoryLimitFromFile(dir string, version cGroupVersion) (uint64, error) {
	var filename string

	switch version {
	case cGroupVersion1:
		filename = "memory.limit_in_bytes"
	case cGroupVersion2:
		filename = "memory.max"
	default:
		return 0, fmt.Errorf("unknown cgroup version %d", version)
	}

	buf, err := os.ReadFile(filepath.Join(dir, filename))
	if err != nil {
		return 0, err
	}

	contents := string(buf)
	if strings.TrimSpace(contents) == "max" {
		return 0, errNoLimitSpecified
	}

	return strconv.ParseUint(strings.TrimSpace(contents), 10, 64)
}

// getCGroupMount reads /proc/self/mountinfo and finds the mount point of the cgroup system
func getCGroupMount(system string) (string, cGroupVersion, error) {
	file, err := os.Open("/proc/self/cgroup")
	if err != nil {
		return "", 0, fmt.Errorf("could not open /proc/self/cgroup: %w", err)
	}
	defer file.Close()

	info, err := readCGroupFile(file)
	if err != nil {
		return "", 0, err
	}

	mountPath, err := info.getMountPoint(system)
	if err != nil {
		return "", 0, errNoLimitSpecified
	}

	file, err = os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", 0, fmt.Errorf("could not open /proc/self/mountinfo: %w", err)
	}
	defer file.Close()

	var fs string

	switch info.version {
	case cGroupVersion1:
		fs = "cgroup"
	case cGroupVersion2:
		fs = "cgroup2"
	}

	path, err := readMountInfo(file, mountPath, fs, system)
	if err != nil {
		return "", 0, err
	}

	return path, info.version, nil
}

// getCGroupMemoryLimit finds the memory limit specified by the cgroup by reading the correct file in the VFS based on
// the cgroup version that is detected. If there is no limit or no cgroup is detected then errNoLimitSpecified is
// returned.
func getCGroupMemoryLimit() (uint64, error) {
	path, version, err := getCGroupMount("memory")
	if err != nil {
		return 0, err
	}

	return getCGroupMemoryLimitFromFile(path, version)
}

// readCGroup2CPULimit reads one or two space separated fields from reader where the first field is the maximum usage
// and the optional second field is the period.
func readCGroup2CPULimit(reader io.Reader) (float64, error) {
	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		return 0, err
	}

	// cgroup2 cpu.max has two fields with the last being optional - <max> <period>. Both are uints and dividing them
	// gives the number of CPUs that can be used by the group. As an example:
	// 200000 100000
	// would be 2 CPUs
	fields := strings.Split(string(buf), " ")
	if len(fields) == 0 {
		return 0, errNoLimitSpecified
	}

	if strings.TrimSpace(fields[0]) == "max" {
		return 0, errNoLimitSpecified
	}

	if len(fields) > 2 {
		return 0, errNoLimitSpecified
	}

	period := defaultCPUPeriod
	if len(fields) == 2 {
		period, err = strconv.ParseUint(strings.TrimSpace(fields[1]), 10, 64)
		if err != nil {
			return 0, errNoLimitSpecified
		}
	}

	allowance, err := strconv.ParseUint(strings.TrimSpace(fields[0]), 10, 64)
	if err != nil {
		return 0, errNoLimitSpecified
	}

	return float64(allowance) / float64(period), nil
}

func getCGroup2CPULimitFromFile(dir string) (float64, error) {
	file, err := os.Open(filepath.Join(dir, "cpu.max"))
	if err != nil {
		return 0, err
	}
	defer file.Close()

	return readCGroup2CPULimit(file)
}

func readUIntFromFile(filename string) (uint64, error) {
	buf, err := os.ReadFile(filename)
	if err != nil {
		return 0, err
	}

	v, err := strconv.ParseUint(strings.TrimSpace(string(buf)), 10, 64)
	if err != nil {
		return 0, errNoLimitSpecified
	}

	return v, nil
}

func getCGroup1CPULimitFromFiles(dir string) (float64, error) {
	period, err := readUIntFromFile(filepath.Join(dir, "cpu.cfs_period_us"))
	if err != nil {
		return 0, errNoLimitSpecified
	}

	quota, err := readUIntFromFile(filepath.Join(dir, "cpu.cfs_quota_us"))
	if err != nil {
		return 0, errNoLimitSpecified
	}

	return float64(quota) / float64(period), nil
}

// getCPUCGroupMount will find the mount point of the CPU system
func getCPUCGroupMount() (string, cGroupVersion, error) {
	// Sometimes cpu is grouped with cpuacct and sometime not. Check cpu,cpuacct first and then fallback to cpu
	path, version, err := getCGroupMount("cpu,cpuacct")
	if err == nil {
		return path, version, nil
	}

	if !errors.Is(err, errNoLimitSpecified) {
		return "", 0, err
	}

	path, version, err = getCGroupMount("cpu")
	if err != nil {
		return "", 0, err
	}

	return path, version, err
}

// getCGroupCPULimit will find the CPU usage limit defined for the current cgroup if there is one.
func getCGroupCPULimit() (float64, error) {
	path, version, err := getCPUCGroupMount()
	if err != nil {
		return 0, err
	}

	switch version {
	case cGroupVersion1:
		return getCGroup1CPULimitFromFiles(path)
	case cGroupVersion2:
		return getCGroup2CPULimitFromFile(path)
	}

	return 0, fmt.Errorf("unknown cgroup version %d", version)
}
