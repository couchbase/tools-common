package util

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// FileExists returns a boolean indicating whether a file at the provided path exists.
func FileExists(path string) (bool, error) {
	stats, err := os.Stat(path)
	if err != nil {
		return false, ignoreINE(err)
	}

	if stats.IsDir() {
		return false, ErrNotFile
	}

	return true, nil
}

// FileSize returns the size in bytes of the file at the provided path.
func FileSize(path string) (uint64, error) {
	stats, err := os.Stat(path)
	if err != nil {
		return 0, err
	}

	return uint64(stats.Size()), nil
}

// Touch is analogous to the unix 'touch' command, it updates the access/modified times of the file at the provided path
// creating it if it doesn't exist.
func Touch(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE, DefaultFileMode)
	if err != nil {
		return err
	}
	defer file.Close()

	now := time.Now()

	err = os.Chtimes(path, now, now)
	if err != nil {
		return err
	}

	return file.Sync()
}

// Create a new file at the provided path in read/write mode, any existing file will be truncated when opening.
func Create(path string) (*os.File, error) {
	return CreateFile(path, os.O_RDWR, 0)
}

// CreateFile creates a new file (or truncates an existing one) at the provided path using the given flags/mode.
//
// NOTE: If a zero value file mode is suppled, the default will be used.
func CreateFile(path string, flags int, mode os.FileMode) (*os.File, error) {
	if mode == 0 {
		mode = DefaultFileMode
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|flags, mode)
	if err != nil {
		return nil, err
	}

	// The files mode may not be exactly what we provided due to a umask, we should update the permissions to be sure.
	err = file.Chmod(mode)
	if err == nil {
		return file, nil
	}

	file.Close()

	return nil, err
}

// WriteAt writes data to the given offset in the file at the provided path.
//
// NOTE: The file at the given path must already exist.
func WriteAt(path string, data []byte, offset int64) error {
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteAt(data, offset)
	if err != nil {
		return err
	}

	return file.Sync()
}

// WriteFile writes out the provided data to the file at the given path.
func WriteFile(path string, data []byte, mode os.FileMode) error {
	file, err := CreateFile(path, os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	return file.Sync()
}

// WriteTempFile writes out the provided data to a temporary file in the given directory (OS temporary directory if
// omitted) and returns the path to that file.
//
// NOTE: It's the job of the caller to correctly handle cleaning up the file.
func WriteTempFile(dir string, data []byte) (string, error) {
	if dir == "" {
		dir = os.TempDir()
	}

	file, err := os.CreateTemp(dir, "temporary_")
	if err != nil {
		return "", err
	}

	_, err = file.Write(data)
	if err != nil {
		return "", err
	}

	err = file.Sync()
	if err != nil {
		return "", err
	}

	return file.Name(), nil
}

// WriteToFile copies all the data from the provided reader and writes it to the file at the given path.
func WriteToFile(path string, reader io.Reader, mode os.FileMode) error {
	file, err := CreateFile(path, os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, reader)
	if err != nil {
		return err
	}

	return file.Sync()
}

// CopyFile copies the source file to the sink file truncating it if it already exists.
//
// NOTE: The sink file will be given the same permissions as the source file.
func CopyFile(source, sink string) error {
	stats, err := os.Stat(source)
	if err != nil {
		return err
	}

	file, err := CreateFile(sink, os.O_WRONLY, stats.Mode())
	if err != nil {
		return err
	}
	defer file.Close()

	return CopyFileTo(source, file)
}

// CopyFileTo copies all the data from the file at the provided path into the given writer.
func CopyFileTo(path string, writer io.Writer) error {
	return CopyFileRangeTo(path, 0, 0, writer)
}

// CopyFileRangeTo is similar to 'CopyFileTo' but allows specifying an offset/length.
func CopyFileRangeTo(path string, offset, length int64, writer io.Writer) error {
	file, err := OpenSeqAccess(path, offset, length)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}

	if length == 0 {
		_, err = io.Copy(writer, file)
	} else {
		_, err = io.CopyN(writer, file, length)
	}

	return err
}

// ReadIfProvided is the equvilent to 'os.ReadFile' but will return <nil>, <nil> if no path is provided.
func ReadIfProvided(path string) ([]byte, error) {
	if path == "" {
		return nil, nil
	}

	return os.ReadFile(path)
}

// ReadJSONFile unmarshals data from the provided file into the given interface.
func ReadJSONFile(path string, data any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return json.NewDecoder(file).Decode(&data)
}

// WriteJSONFile marshals the provided interface and writes it to a file at the given path.
func WriteJSONFile(path string, data any, mode os.FileMode) error {
	file, err := CreateFile(path, os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer file.Close()

	err = json.NewEncoder(file).Encode(data)
	if err != nil {
		return err
	}

	return file.Sync()
}

// Sync the provided file to disk.
func Sync(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return file.Sync()
}

// Atomic will perform the provided function in an "atmoic" fashion. It's required that the provided function create the
// file at the given path if it doesn't already exist.
//
// NOTE: This only works to the degree that the underlying operating system guarantees that renames are atomic.
func Atomic(path string, fn func(path string) error) error {
	file, err := os.CreateTemp(filepath.Dir(path), fmt.Sprintf("temporary_%s_", filepath.Base(path)))
	if err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}

	err = fn(file.Name())
	if err != nil {
		return err
	}

	return os.Rename(file.Name(), path)
}
