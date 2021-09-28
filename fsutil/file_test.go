package fsutil

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileExists(t *testing.T) {
	t.Run("Exists", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, Touch(path))

		exists, err := FileExists(path)
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("NotExists", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		exists, err := FileExists(path)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("NotAFile", func(t *testing.T) {
		testDir := t.TempDir()

		exists, err := FileExists(testDir)
		require.ErrorIs(t, err, ErrNotFile)
		require.False(t, exists)
	})
}

func TestFileSize(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, Touch(path))

		size, err := FileSize(path)
		require.NoError(t, err)
		require.Zero(t, size)
	})

	t.Run("NotEmpty", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, WriteFile(path, make([]byte, 4096), 0))

		size, err := FileSize(path)
		require.NoError(t, err)
		require.Equal(t, uint64(4096), size)
	})
}

func TestTouch(t *testing.T) {
	t.Run("Exists", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, WriteFile(path, make([]byte, 4096), 0))
		require.FileExists(t, path)

		s1, err := os.Stat(path)
		require.NoError(t, err)

		require.NoError(t, Touch(path))

		s2, err := os.Stat(path)
		require.NoError(t, err)

		require.Equal(t, s1.Size(), s2.Size())
		require.NotEqual(t, s1.ModTime(), s2.ModTime())
		require.NotEqual(t, s1.ModTime(), s2.ModTime())
	})

	t.Run("NotExists", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, Touch(path))
		require.FileExists(t, path)
	})
}

func TestCreate(t *testing.T) {
	t.Run("Exists", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, Touch(path))

		file, err := Create(path)
		require.NoError(t, err)
		require.NoError(t, file.Close())

		stats, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, DefaultFileMode, stats.Mode())
		require.Equal(t, int64(0), stats.Size())
	})

	t.Run("NotExists", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		file, err := Create(path)
		require.NoError(t, err)
		require.NoError(t, file.Close())

		stats, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, DefaultFileMode, stats.Mode())
		require.Equal(t, int64(0), stats.Size())
	})

	t.Run("Truncate", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, WriteFile(path, make([]byte, 4096), 0o777))

		stats, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o777), stats.Mode())
		require.Equal(t, int64(4096), stats.Size())

		file, err := Create(path)
		require.NoError(t, err)
		require.NoError(t, file.Close())

		stats, err = os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, DefaultFileMode, stats.Mode())
		require.Equal(t, int64(0), stats.Size())
	})
}

func TestCreateFile(t *testing.T) {
	t.Run("Exists", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, Touch(path))

		file, err := CreateFile(path, 0, 0)
		require.NoError(t, err)
		require.NoError(t, file.Close())

		stats, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, DefaultFileMode, stats.Mode())
		require.Equal(t, int64(0), stats.Size())
	})

	t.Run("NotExistsDefaultMode", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		file, err := CreateFile(path, 0, 0)
		require.NoError(t, err)
		require.NoError(t, file.Close())

		stats, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, DefaultFileMode, stats.Mode())
		require.Equal(t, int64(0), stats.Size())
	})

	t.Run("NotExistsProvidedMode", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		file, err := CreateFile(path, 0, 0o777)
		require.NoError(t, err)
		require.NoError(t, file.Close())

		stats, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o777), stats.Mode())
		require.Equal(t, int64(0), stats.Size())
	})

	t.Run("Truncate", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, WriteFile(path, make([]byte, 4096), 0o777))

		stats, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o777), stats.Mode())
		require.Equal(t, int64(4096), stats.Size())

		file, err := CreateFile(path, 0, 0)
		require.NoError(t, err)
		require.NoError(t, file.Close())

		stats, err = os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, DefaultFileMode, stats.Mode())
		require.Equal(t, int64(0), stats.Size())
	})
}

func TestWriteAt(t *testing.T) {
	t.Run("ValidOffset", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, WriteFile(path, []byte("Hello, World"), 0))
		require.NoError(t, WriteAt(path, []byte("!"), int64(len([]byte("Hello, World")))))

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello, World!"), data)
	})

	// I'm not 100% sure of the behavior on Windows, but on Linux when writing to an offset greater than the size of the
	// file, the existing space will be padded with zeros (i.e. creating a sparse file).
	t.Run("LargerOffset", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, Touch(path))
		require.NoError(t, WriteAt(path, []byte("Hello, World!"), 4096))

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, append(make([]byte, 4096), []byte("Hello, World!")...), data)
	})
}

func TestWriteFile(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(strconv.FormatBool(exists), func(t *testing.T) {
			var (
				testDir = t.TempDir()
				path    = filepath.Join(testDir, "file")
			)

			if exists {
				require.NoError(t, os.WriteFile(path, []byte("<existing data>"), 0o777))
			}

			require.NoError(t, WriteFile(path, []byte("Hello, World!"), 0))

			stats, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, DefaultFileMode, stats.Mode())
			require.Equal(t, int64(13), stats.Size())

			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello, World!"), data)
		})
	}
}

func TestWriteTempFileUseProvidedDir(t *testing.T) {
	testDir := t.TempDir()

	path, err := WriteTempFile(testDir, []byte("Hello, World!"))
	require.NoError(t, err)
	require.Equal(t, testDir, filepath.Dir(path))
	require.FileExists(t, path)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello, World!"), data)

	require.NoError(t, Remove(path, false))
	require.NoFileExists(t, path)
}

func TestWriteTempFileUseOSTempDir(t *testing.T) {
	path, err := WriteTempFile("", []byte("Hello, World!"))
	require.NoError(t, err)
	require.Equal(t, os.TempDir(), filepath.Dir(path))
	require.FileExists(t, path)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello, World!"), data)

	require.NoError(t, Remove(path, false))
	require.NoFileExists(t, path)
}

func TestWriteToFile(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(strconv.FormatBool(exists), func(t *testing.T) {
			var (
				testDir = t.TempDir()
				path    = filepath.Join(testDir, "file")
			)

			if exists {
				require.NoError(t, os.WriteFile(path, []byte("<existing data>"), 0o777))
			}

			require.NoError(t, WriteToFile(path, bytes.NewReader([]byte("Hello, World!")), 0))

			stats, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, DefaultFileMode, stats.Mode())
			require.Equal(t, int64(13), stats.Size())

			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello, World!"), data)
		})
	}
}

func TestCopyFile(t *testing.T) {
	t.Run("Exists", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			source  = filepath.Join(testDir, "source")
			sink    = filepath.Join(testDir, "sink")
		)

		require.NoError(t, WriteFile(source, []byte("Hello, World!"), 0))
		require.NoError(t, WriteFile(sink, []byte("Goodbye, Cruel World!"), 0o777))

		require.NoError(t, CopyFile(source, sink))
		require.FileExists(t, sink)

		stats, err := os.Stat(sink)
		require.NoError(t, err)
		require.Equal(t, DefaultFileMode, stats.Mode())

		data, err := os.ReadFile(sink)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello, World!"), data)
	})

	t.Run("NotExists", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			source  = filepath.Join(testDir, "source")
			sink    = filepath.Join(testDir, "sink")
		)

		require.NoError(t, WriteFile(source, []byte("Hello, World!"), 0))

		require.NoError(t, CopyFile(source, sink))

		stats, err := os.Stat(sink)
		require.NoError(t, err)
		fmt.Println(DefaultFileMode)
		fmt.Println(stats.Mode())
		require.Equal(t, DefaultFileMode, stats.Mode())

		data, err := os.ReadFile(sink)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello, World!"), data)
	})

	t.Run("NonDefaultMode", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			source  = filepath.Join(testDir, "source")
			sink    = filepath.Join(testDir, "sink")
		)

		require.NoError(t, WriteFile(source, []byte("Hello, World!"), 0o777))

		require.NoError(t, CopyFile(source, sink))

		stats, err := os.Stat(sink)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o777), stats.Mode())

		data, err := os.ReadFile(sink)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello, World!"), data)
	})
}

func TestCopyFileTo(t *testing.T) {
	var (
		testDir = t.TempDir()
		path    = filepath.Join(testDir, "file")
		buffer  bytes.Buffer
	)

	require.NoError(t, WriteFile(path, []byte("Hello, World!"), 0))
	require.NoError(t, CopyFileTo(path, &buffer))
	require.Equal(t, []byte("Hello, World!"), buffer.Bytes())
}

func TestCopyFileRangeTo(t *testing.T) {
	type test struct {
		name     string
		offset   int64
		length   int64
		expected []byte
	}

	tests := []*test{
		{
			name:     "ZeroOffset",
			expected: []byte("Hello, World!"),
		},
		{
			name:     "NonZeroOffset",
			offset:   7,
			expected: []byte("World!"),
		},
		{
			name:     "ZeroLength",
			expected: []byte("Hello, World!"),
		},
		{
			name:     "NonZeroLength",
			length:   5,
			expected: []byte("Hello"),
		},
		{
			name:     "BothNonZero",
			offset:   5,
			length:   1,
			expected: []byte(","),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				testDir = t.TempDir()
				path    = filepath.Join(testDir, "file")
				buffer  bytes.Buffer
			)

			require.NoError(t, WriteFile(path, []byte("Hello, World!"), 0))
			require.NoError(t, CopyFileRangeTo(path, test.offset, test.length, &buffer))
			require.Equal(t, test.expected, buffer.Bytes())
		})
	}
}

func TestReadJSONFile(t *testing.T) {
	var (
		testDir = t.TempDir()
		path    = filepath.Join(testDir, "file")
	)

	require.NoError(t, WriteFile(path, []byte(`{"key":"value"}`), 0))

	var actual map[string]string

	require.NoError(t, ReadJSONFile(path, &actual))
	require.Equal(t, map[string]string{"key": "value"}, actual)
}

func TestWriteJSONFile(t *testing.T) {
	t.Run("Exists", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, Touch(path))
		require.NoError(t, WriteJSONFile(path, map[string]string{"key": "value"}, 0))

		stats, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, DefaultFileMode, stats.Mode())

		var actual map[string]string

		require.NoError(t, ReadJSONFile(path, &actual))
		require.Equal(t, map[string]string{"key": "value"}, actual)
	})

	t.Run("NotExistsDefaultMode", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, WriteJSONFile(path, map[string]string{"key": "value"}, 0))

		stats, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, DefaultFileMode, stats.Mode())

		var actual map[string]string

		require.NoError(t, ReadJSONFile(path, &actual))
		require.Equal(t, map[string]string{"key": "value"}, actual)
	})

	t.Run("NotExistsProvidedMode", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, WriteJSONFile(path, map[string]string{"key": "value"}, 0o777))

		stats, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o777), stats.Mode())

		var actual map[string]string

		require.NoError(t, ReadJSONFile(path, &actual))
		require.Equal(t, map[string]string{"key": "value"}, actual)
	})

	t.Run("Truncate", func(t *testing.T) {
		var (
			testDir = t.TempDir()
			path    = filepath.Join(testDir, "file")
		)

		require.NoError(t, WriteFile(path, []byte("<existing data>"), 0o777))
		require.NoError(t, WriteJSONFile(path, map[string]string{"key": "value"}, 0))

		stats, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, DefaultFileMode, stats.Mode())

		var actual map[string]string
		require.NoError(t, ReadJSONFile(path, &actual))
		require.Equal(t, map[string]string{"key": "value"}, actual)
	})
}

// This is only a smoke/sanity test, we can't really validate that this is behaving as expected
func TestSync(t *testing.T) {
	var (
		testDir = t.TempDir()
		path    = filepath.Join(testDir, "file")
	)

	require.NoError(t, Touch(path))
	require.NoError(t, Sync(path))
}

// This is just a sanity test, we can't really validate any platform specific behavior.
func TestOpenSeqAccess(t *testing.T) {
	var (
		testDir = t.TempDir()
		path    = filepath.Join(testDir, "file")
	)

	require.NoError(t, Touch(path))

	file, err := OpenSeqAccess(path, 0, 0)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

// This is just a sanity test, we can't really validate any platform specific behavior.
func TestOpenRandAccess(t *testing.T) {
	var (
		testDir = t.TempDir()
		path    = filepath.Join(testDir, "file")
	)

	require.NoError(t, Touch(path))

	file, err := OpenRandAccess(path, 0, 0)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

func TestAtomic(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(strconv.FormatBool(exists), func(t *testing.T) {
			var (
				testDir = t.TempDir()
				path    = filepath.Join(testDir, "file")
			)

			if exists {
				require.NoError(t, os.WriteFile(path, []byte("<existing data>"), 0o777))
			}

			err := Atomic(path, func(path string) error { return WriteFile(path, []byte("Hello, World!"), 0) })
			require.NoError(t, err)

			stats, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, DefaultFileMode, stats.Mode())
			require.Equal(t, int64(13), stats.Size())

			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello, World!"), data)
		})
	}
}
