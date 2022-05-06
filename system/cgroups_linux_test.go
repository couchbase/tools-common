package system

import (
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCGroupReadFileV1(t *testing.T) {
	v1Tests := []struct {
		file, name  string
		mountPoints map[string]string
		errors      bool
	}{
		{
			name: "valid",
			file: `3:misc:/
12:rdma:/
11:perf_event:/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476
10:devices:/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476
9:cpu,cpuacct:/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476
8:memory:/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476
7:pids:/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476
6:net_cls,net_prio:/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476
5:freezer:/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476
4:cpuset:/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476
3:hugetlb:/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476
2:blkio:/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476
1:name=systemd:/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476
0::/system.slice/containerd.service
`,

			mountPoints: map[string]string{
				"perf_event":       "/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476",
				"devices":          "/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476",
				"cpu,cpuacct":      "/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476",
				"memory":           "/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476",
				"pids":             "/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476",
				"net_cls,net_prio": "/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476",
				"freezer":          "/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476",
				"cpuset":           "/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476",
				"hugetlb":          "/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476",
				"blkio":            "/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476",
				"name=systemd":     "/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476",
				"rdma":             "/",
				"misc":             "/",
			},
		},
		{
			name: "invalid-line-in-middle",
			file: `3:misc:/
12:rdma:/
11:perf_event:/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476
I AM NOT A VALID LINE
5:freezer:/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476
4:cpuset:/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476
`,
			errors: true,
		},
	}

	for _, test := range v1Tests {
		t.Run(test.name, func(t *testing.T) {
			info, err := readCGroupFile(strings.NewReader(test.file))
			if test.errors {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, &cGroupInfo{
				version:     cGroupVersion1,
				mountPoints: test.mountPoints,
			}, info)
		})
	}
}

func TestCGroupReadFileV2(t *testing.T) {
	v2Tests := []struct {
		file, name, mountPoint string
		errors                 bool
	}{
		{name: "root mount", mountPoint: "/", file: "0::/\n"},
		{name: "more complicated mount", mountPoint: "/foo/bar/baz", file: "0::/foo/bar/baz\n"},
		{name: "no separator", errors: true, file: "0  /foo/bar/baz\n"},
		{name: "no mount point", errors: true, file: "0::\n"},
	}

	for _, test := range v2Tests {
		t.Run(test.name, func(t *testing.T) {
			info, err := readCGroupFile(strings.NewReader(test.file))
			if test.errors {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, &cGroupInfo{
				version:     cGroupVersion2,
				mountPoints: map[string]string{"default": test.mountPoint},
			}, info)
		})
	}
}

func TestCGroupReadMountInfo(t *testing.T) {
	mountPoint := "/docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476"
	//nolint:lll
	file := `660 963 0:57 / / rw,relatime master:462 - overlay overlay rw,lowerdir=/var/lib/docker/overlay2/l/ZN3MFUAF6R6MBVCCKZ4QCVN5GT:/var/lib/docker/overlay2/l/BP2UJJ4QGD3RNG7YTZR7ZT34XB,upperdir=/var/lib/docker/overlay2/6fa4993d7b2a089ab2a791f508b029bf9bd53cd1cf7243a2299604c65e31da48/diff,workdir=/var/lib/docker/overlay2/6fa4993d7b2a089ab2a791f508b029bf9bd53cd1cf7243a2299604c65e31da48/work
1661 1660 0:68 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
1662 1660 0:69 / /dev rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755,inode64
1663 1662 0:70 / /dev/pts rw,nosuid,noexec,relatime - devpts devpts rw,gid=5,mode=620,ptmxmode=666
1664 1660 0:71 / /sys ro,nosuid,nodev,noexec,relatime - sysfs sysfs ro
1665 1664 0:72 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,mode=755,inode64
1666 1665 0:29 /docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476 /sys/fs/cgroup/systemd ro,nosuid,nodev,noexec,relatime master:11 - cgroup cgroup rw,xattr,name=systemd
1667 1665 0:32 /docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476 /sys/fs/cgroup/blkio ro,nosuid,nodev,noexec,relatime master:15 - cgroup cgroup rw,blkio
1668 1665 0:33 /docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476 /sys/fs/cgroup/hugetlb ro,nosuid,nodev,noexec,relatime master:16 - cgroup cgroup rw,hugetlb
1669 1665 0:34 /docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476 /sys/fs/cgroup/cpuset ro,nosuid,nodev,noexec,relatime master:17 - cgroup cgroup rw,cpuset
1670 1665 0:35 /docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476 /sys/fs/cgroup/freezer ro,nosuid,nodev,noexec,relatime master:18 - cgroup cgroup rw,freezer
1671 1665 0:36 /docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476 /sys/fs/cgroup/net_cls,net_prio ro,nosuid,nodev,noexec,relatime master:19 - cgroup cgroup rw,net_cls,net_prio
1672 1665 0:37 /docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476 /sys/fs/cgroup/pids ro,nosuid,nodev,noexec,relatime master:20 - cgroup cgroup rw,pids
1673 1665 0:38 /docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476 /sys/fs/cgroup/memory ro,nosuid,nodev,noexec,relatime master:21 - cgroup cgroup rw,memory
1674 1665 0:39 /docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476 /sys/fs/cgroup/cpu,cpuacct ro,nosuid,nodev,noexec,relatime master:22 - cgroup cgroup rw,cpu,cpuacct
1675 1665 0:40 /docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476 /sys/fs/cgroup/devices ro,nosuid,nodev,noexec,relatime master:23 - cgroup cgroup rw,devices
1676 1665 0:41 /docker/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476 /sys/fs/cgroup/perf_event ro,nosuid,nodev,noexec,relatime master:24 - cgroup cgroup rw,perf_event
1677 1665 0:42 / /sys/fs/cgroup/rdma ro,nosuid,nodev,noexec,relatime master:25 - cgroup cgroup rw,rdma
1678 1665 0:43 / /sys/fs/cgroup/misc ro,nosuid,nodev,noexec,relatime master:26 - cgroup cgroup rw,misc
1679 1662 0:67 / /dev/mqueue rw,nosuid,nodev,noexec,relatime - mqueue mqueue rw
1680 1662 0:73 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw,size=65536k,inode64
1681 1660 8:3 /var/lib/docker/containers/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476/resolv.conf /etc/resolv.conf rw,relatime - ext4 /dev/sda3 rw,errors=remount-ro
1682 1660 8:3 /var/lib/docker/containers/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476/hostname /etc/hostname rw,relatime - ext4 /dev/sda3 rw,errors=remount-ro
1683 1660 8:3 /var/lib/docker/containers/51722a12fe5ccb03046f01753c92fc06172a5343c6267ec286731ea9e559c476/hosts /etc/hosts rw,relatime - ext4 /dev/sda3 rw,errors=remount-ro
964 1662 0:70 /0 /dev/console rw,nosuid,noexec,relatime - devpts devpts rw,gid=5,mode=620,ptmxmode=666
965 1661 0:68 /bus /proc/bus ro,nosuid,nodev,noexec,relatime - proc proc rw
966 1661 0:68 /fs /proc/fs ro,nosuid,nodev,noexec,relatime - proc proc rw
967 1661 0:68 /irq /proc/irq ro,nosuid,nodev,noexec,relatime - proc proc rw
977 1661 0:68 /sys /proc/sys ro,nosuid,nodev,noexec,relatime - proc proc rw
978 1661 0:68 /sysrq-trigger /proc/sysrq-trigger ro,nosuid,nodev,noexec,relatime - proc proc rw
1070 1661 0:74 / /proc/asound ro,relatime - tmpfs tmpfs ro,inode64
1071 1661 0:75 / /proc/acpi ro,relatime - tmpfs tmpfs ro,inode64
1176 1661 0:69 /null /proc/kcore rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755,inode64
1177 1661 0:69 /null /proc/keys rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755,inode64
1279 1661 0:69 /null /proc/timer_list rw,nosuid - tmpfs tmpfs rw,size=65536k,mode=755,inode64
1280 1661 0:76 / /proc/scsi ro,relatime - tmpfs tmpfs ro,inode64
`
	tests := []struct {
		name, mountPoint, file, fs, contains string
		found, errors                        bool
	}{
		{name: "valid-memory", file: file, fs: "cgroup", contains: "memory", found: true, mountPoint: mountPoint},
		{name: "valid-cpu", file: file, fs: "cgroup", contains: "cpuset", found: true, mountPoint: mountPoint},
		{name: "not-found-contains", file: file, fs: "cgroup", contains: "xyz", mountPoint: mountPoint},
		{name: "not-found-mount-point", file: file, fs: "cgroup", contains: "mem", mountPoint: "/foo/bar/baz"},
		{name: "invalid-empty", file: "", fs: "cgroup"},
		{name: "invalid-not-enough-fields", file: "660 963 0:57\n", errors: true},
		{
			name:   "invalid-no-separator",
			file:   "1661 1660 0:68 / /proc rw,nosuid,nodev,noexec,relatime  proc proc rw\n",
			errors: true,
		},
		{
			name: "multiple-separators",
			//nolint:lll
			file:     "1495 1465 0:65 / /run/user/1000/gvfs rw,nosuid,nodev,relatime shared:828 - fuse.gvfsd-fuse gvfsd-fuse rw,user_id=1000,group_id=1000\n",
			fs:       "fuse.gvfsd-fuse",
			contains: "not-found",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mount, err := readMountInfo(strings.NewReader(test.file), test.mountPoint, test.fs, test.contains)
			if test.errors {
				require.NotErrorIs(t, err, errNoLimitSpecified)
				return
			}

			if !test.found {
				require.ErrorIs(t, err, errNoLimitSpecified)
				return
			}

			require.NoError(t, err)
			require.Equal(t, path.Join("/sys/fs/cgroup", test.contains), mount)
		})
	}
}
