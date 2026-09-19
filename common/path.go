package common

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/nextbillion-ai/gsg/logger"
)

const (
	module = "COMMON"
)

// ParseURL parses url into scheme, bucket, prefix
func ParseURL(input string) (string, string, string) {
	u, err := url.Parse(input)
	if err != nil {
		logger.Debug("parseurl", "failed with %s", err)
	}
	// from gs or s3
	if len(u.Scheme) > 0 {
		return u.Scheme, u.Host, strings.TrimLeft(u.Path, "/")
	}
	// from local
	return u.Scheme, u.Host, u.Path
}

// ParseFile parses file into dir, name
func ParseFile(input string) (string, string) {
	dir := filepath.Dir(input)
	dir = strings.ReplaceAll(dir, ":/", "://")
	name := filepath.Base(input)
	return dir, name
}

// JoinPath combines dir and parts into a new path
func JoinPath(dir string, parts ...string) string {
	items := []string{dir}
	items = append(items, parts...)
	path := filepath.Join(items...)
	path = strings.ReplaceAll(path, ":/", "://")
	return path
}

// GetRelativePath gets the relative path given path and directory
func GetRelativePath(dir, path string) string {
	relativePath := strings.TrimPrefix(path, dir)
	if relativePath == path {
		return relativePath
	}
	return strings.TrimLeft(relativePath, "/")
}

// SetPrefixAsDirectory sets the prefix as directory
func SetPrefixAsDirectory(prefix string) string {
	if len(prefix) == 0 {
		return prefix
	}
	prefix = strings.TrimRight(prefix, "/")
	return prefix + "/"
}

// GetDstPath gets the destination path
func GetDstPath(srcPrefix, srcPath, dstPrefix string) string {
	relativePath := GetRelativePath(srcPrefix, srcPath)
	dstPath := JoinPath(dstPrefix, relativePath)
	return dstPath
}

// GetLocalDstPath is GetDstPath for a file written from an object: the result
// has to stay inside dstPrefix. See JoinLocalPath.
func GetLocalDstPath(srcPrefix, srcPath, dstPrefix string) (string, error) {
	return JoinLocalPath(dstPrefix, GetRelativePath(srcPrefix, srcPath))
}

// JoinLocalPath joins a path taken from an object name onto a local directory,
// and refuses it when the result would not be inside that directory.
//
// A listing returns object names verbatim and a service accepts ".." in them,
// so "src/../esc/f" copied from "src" into "dst" would otherwise be written to
// "dst/../esc/f" -- wherever the name points, as far up as it cares to climb.
func JoinLocalPath(dir, rel string) (string, error) {
	joined := JoinPath(dir, rel)
	inside, err := filepath.Rel(filepath.Clean(dir), filepath.Clean(joined))
	if err != nil || inside == ".." || strings.HasPrefix(inside, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("refusing to write %q: it would land outside %s", rel, dir)
	}
	return joined, nil
}

// IsSubPath checks if a path is under another one in directory manner
// case 1: gs://abc/def -> gs://abc/def : true
// case 2: gs://abc/def -> gs://abc/de : false
// case 3: gs://abc/def -> gs://abc/def/ : false
// case 4: gs://abc/def/-> gs://abc/def/ : true
// case 5: gs://abc/def/ghi -> gs://abc/def : true
// case 6: gs://abc/def/ghi -> gs://abc/de : false
// case 7: gs://abc/def/ghi -> gs://abc/def/ : true
func IsSubPath(subPath, path string) bool {
	if !strings.HasPrefix(subPath, path) {
		return false
	}
	parts1 := strings.Split(subPath, "/")
	parts2 := strings.Split(path, "/")
	l2 := len(parts2) - 1
	if len(parts2[l2]) > 0 && len(parts2[l2]) < len(parts1[l2]) {
		return false
	}
	return true
}
