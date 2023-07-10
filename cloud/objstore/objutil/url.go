package objutil

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/couchbase/tools-common/cloud/objstore/objval"
)

// ErrInvalidCloudPath returns if the user has incorrectly used the cloud style scheme prefixed argument; the error
// message indicates/display the correct usage to the user.
type ErrInvalidCloudPath struct {
	prefix string
}

func (e *ErrInvalidCloudPath) Error() string {
	medium := "BUCKET"
	if e.prefix == "az://" {
		medium = "CONTAINER"
	}

	return fmt.Sprintf("invalid use of the '%s' prefix expected the format '%s${%s_NAME}/${PATH}' "+
		"where '%s${%s_NAME}' is also valid", e.prefix, e.prefix, medium, e.prefix, medium)
}

// CloudOrFileURL represents a cloud storage url (eg s3://bucket/path/to/file.txt) or a local path.
type CloudOrFileURL struct {
	Provider objval.Provider
	Bucket   string
	Path     string
}

func (u *CloudOrFileURL) String() string {
	if u.Provider == objval.ProviderNone {
		return fmt.Sprintf("file://%s", u.Path)
	}

	return fmt.Sprintf("%s%s/%s", u.Provider.ToScheme(), u.Bucket, u.Path)
}

// Join returns a new CloudOrFileURL with args appended to u.
func (u *CloudOrFileURL) Join(args ...string) *CloudOrFileURL {
	parts := []string{u.Path}
	parts = append(parts, args...)

	return &CloudOrFileURL{Bucket: u.Bucket, Path: path.Join(parts...), Provider: u.Provider}
}

// respectTrailingSeparator will add a trailing sep to current if original has one and remove it if current has one but
// original does not.
func respectTrailingSeparator(original, current, sep string) string {
	if strings.HasSuffix(original, sep) && !strings.HasSuffix(current, sep) {
		return current + sep
	}

	if !strings.HasSuffix(original, sep) && strings.HasSuffix(current, sep) {
		return strings.TrimSuffix(current, sep)
	}

	return current
}

// parseFileURL parses argument into a CloudOrFileURL, making the path absolute.
//
// NOTE: Assumes argument has no scheme prefix or a file:// one.
func parseFileURL(argument string) (*CloudOrFileURL, error) {
	absPath, err := filepath.Abs(strings.TrimPrefix(argument, "file://"))
	if err != nil {
		return nil, err
	}

	return &CloudOrFileURL{
		Path:     respectTrailingSeparator(argument, absPath, string(os.PathSeparator)),
		Provider: objval.ProviderNone,
	}, nil
}

// parseCloudURL parses argument into a CloudOrFileURL, making sure it has a valid cloud scheme.
func parseCloudURL(argument, prefix string) (*CloudOrFileURL, error) {
	var (
		provider  objval.Provider
		supported = []string{"file://", "s3://", "az://", "gs://"}
	)

	switch prefix {
	case "az://":
		provider = objval.ProviderAzure
	case "gs://":
		provider = objval.ProviderGCP
	case "s3://":
		provider = objval.ProviderAWS
	default:
		return nil, fmt.Errorf("cloud prefix provided for an unsupported cloud provider, expected [%s]",
			strings.Join(supported, ", "))
	}

	split := strings.Split(strings.TrimPrefix(argument, prefix), "/")
	if len(split) == 0 || split[0] == "" {
		return nil, &ErrInvalidCloudPath{prefix: prefix}
	}

	return &CloudOrFileURL{
		Bucket:   split[0],
		Path:     respectTrailingSeparator(argument, path.Join(split[1:]...), "/"),
		Provider: provider,
	}, nil
}

// ParseCloudOrFileURL parses a URL which is either a file path or a cloud path. It will automatically convert a local
// path into an absolute path using the 'fsutil.ConvertToAbsolutePath' function.
func ParseCloudOrFileURL(argument string) (*CloudOrFileURL, error) {
	var prefix string

	idx := strings.Index(argument, "://")
	if idx > 0 {
		prefix = argument[:idx+len("://")]
	}

	if prefix == "" || prefix == "file://" {
		return parseFileURL(argument)
	}

	return parseCloudURL(argument, prefix)
}
