package fsutil

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
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

// CreateFile creates a new function (or truncates an existing one) at the provided path using the given flags/mode.
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
func WriteAt(path string, data []byte, offset int64) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, DefaultFileMode)
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

// ReadJSONFile unmarshals data from the provided file into the given interface.
func ReadJSONFile(path string, data interface{}) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return json.NewDecoder(file).Decode(&data)
}

// WriteJSONFile marshals the provided interface and writes it to a file at the given path.
func WriteJSONFile(path string, data interface{}, mode os.FileMode) error {
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
	temp, err := temporaryPath(path)
	if err != nil {
		return err
	}

	err = fn(temp)
	if err != nil {
		return err
	}

	return os.Rename(temp, path)
}

// temporaryPath returns a temporary path which resembles the provided path but is not the same; this may be used as a
// temporary file which may be removed/renamed but leaves implicit context as to why the file itself was created.
func temporaryPath(path string) (string, error) {
	var (
		rnd  = rand.NewSource(time.Now().Unix())
		temp = filepath.Join(filepath.Dir(path), fmt.Sprintf(".temporary_%d_%s", rnd.Int63(), filepath.Base(path)))
	)

	exists, err := FileExists(temp)
	if err != nil {
		return "", err
	}

	// We generated a path to a file that already exists, try again
	if exists {
		return temporaryPath(path)
	}

	return temp, nil
}
