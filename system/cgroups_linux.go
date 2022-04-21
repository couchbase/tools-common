package system

import (
	"bufio"
	"errors"
	"fmt"
	"io"
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

// readMountInfo - will look in r for mountPoint with filesystem type fs. Returns where mountPoint is mounted
func readMountInfo(r io.Reader, mountPoint, fs, contains string) (string, error) {
	var (
		err    error
		line   string
		reader = bufio.NewReader(r)
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
		// 7 takes us up to just before the separator
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

		if postSeparator[0] == fs && strings.Contains(separatorSplit[1], contains) {
			return split[4], nil
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

	f, err := os.Open(filepath.Join(dir, filename))
	if err != nil {
		return 0, err
	}
	defer f.Close()

	buf := make([]byte, 128)

	n, err := f.Read(buf)
	if err != nil {
		return 0, err
	}

	contents := string(buf[0:n])
	if contents == "max" {
		return 0, errNoLimitSpecified
	}

	return strconv.ParseUint(strings.TrimSpace(contents), 10, 64)
}

// getCGroupMemoryLimit finds the memory limit specified by the cgroup by reading the correct file in the VFS based on
// the cgroup version that is detected. If there is no limit or no cgroup is detected then errNoLimitSpecified is
// returned.
func getCGroupMemoryLimit() (uint64, error) {
	file, err := os.Open("/proc/self/cgroup")
	if err != nil {
		return 0, fmt.Errorf("could not open /proc/self/cgroup: %w", err)
	}
	defer file.Close()

	info, err := readCGroupFile(file)
	if err != nil {
		return 0, err
	}

	mountPath, err := info.getMountPoint("memory")
	if err != nil {
		return 0, errNoLimitSpecified
	}

	file, err = os.Open("/proc/self/mountinfo")
	if err != nil {
		return 0, fmt.Errorf("could not open /proc/self/mountinf: %w", err)
	}
	defer file.Close()

	var fs string

	switch info.version {
	case cGroupVersion1:
		fs = "cgroup"
	case cGroupVersion2:
		fs = "cgroup2"
	}

	path, err := readMountInfo(file, mountPath, fs, "memory")
	if err != nil {
		return 0, err
	}

	return getCGroupMemoryLimitFromFile(path, info.version)
}
