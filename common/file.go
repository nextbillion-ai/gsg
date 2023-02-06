package common

import (
	"crypto/md5"
	"gsutil-go/logger"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	tempFileSuffix = "_.gstmp"
)

// GetFileModificationTime gets mtime of a file
func GetFileModificationTime(path string) time.Time {
	file, err := os.Stat(path)
	if err != nil {
		logger.Debug("failed with %s", err)
		return time.Time{}
	}
	return file.ModTime()
}

// SetFileModificationTime sets mtime to a file
func SetFileModificationTime(path string, mt time.Time) {
	err := os.Chtimes(path, mt, mt)
	if err != nil {
		logger.Debug("failed with %s", err)
	}
}

// GetWorkDir returns the directory where the executable file put
func GetWorkDir() string {
	ex, err := os.Executable()
	if err != nil {
		logger.Debug("failed with %s", err)
	}
	path := filepath.Dir(ex)
	return path
}

// Chmod changes mod of a path
func Chmod(path string, mod os.FileMode) {
	err := os.Chmod(path, mod)
	if err != nil {
		logger.Debug("failed with %s", path)
	}
}

// IsPathExist determines if a path exists
func IsPathExist(path string) bool {
	if len(path) == 0 {
		return true
	}
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// IsPathDirectory determines if a path is a directory
func IsPathDirectory(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		logger.Debug("failed with %s", path)
		return false
	}
	return fi.IsDir()
}

// IsPathFile determines if a path is a file
func IsPathFile(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		logger.Debug("failed with %s", path)
		return false
	}
	return !fi.IsDir()
}

// GetFileSize gets the size of a file
func GetFileSize(path string) int64 {
	if IsPathDirectory(path) {
		return 0
	}
	fi, err := os.Stat(path)
	if err != nil {
		logger.Debug("failed with %s", path)
		return 0
	}
	return fi.Size()
}

// GetFileCRC32C gets the crc32c of a file
func GetFileCRC32C(path string) uint32 {
	if IsPathDirectory(path) {
		return 0
	}
	file, err := os.Open(path)
	if err != nil {
		logger.Debug("failed with %s", err)
		return 0
	}
	defer func() { _ = file.Close() }()
	crc32q := crc32.MakeTable(crc32.Castagnoli)
	h32 := crc32.New(crc32q)
	_, err = io.Copy(h32, file)
	if err != nil {
		logger.Debug("failed with %s", err)
		return 0
	}
	return h32.Sum32()
}

// GetFileMD5 gets the md5 of a file
func GetFileMD5(path string) []byte {
	if IsPathDirectory(path) {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		logger.Debug("failed with %s", err)
		return nil
	}
	defer func() { _ = file.Close() }()
	hash := md5.New()
	_, err = io.Copy(hash, file)
	if err != nil {
		logger.Debug("failed with %s", err)
		return nil
	}
	return hash.Sum(nil)
}

// IsTempFile checks if a file is temp file
func IsTempFile(path string) bool {
	return strings.HasSuffix(path, tempFileSuffix)
}

// GetTempFile gets a temp file name
func GetTempFile(path string) string {
	if len(path) == 0 {
		return ""
	}
	return path + tempFileSuffix
}

// CreateFolder creates folder on local drive
func CreateFolder(path string) {
	err := os.MkdirAll(path, 0755)
	if err != nil {
		logger.Debug("failed with %s", err)
	}
}

// CreateFile creates an empty file with given length (if size > 0)
func CreateFile(path string, size int64) {
	f, err := os.Create(path)
	if err != nil {
		logger.Debug("failed with %s", err)
		return
	}
	defer func() { _ = f.Close() }()
	if size > 0 {
		err = f.Truncate(size)
		if err != nil {
			logger.Debug("failed with %s", err)
			return
		}
	}
}
