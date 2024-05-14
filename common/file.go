package common

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nextbillion-ai/gsg/logger"
)

const (
	tempFileSuffix = "_.gstmp"
)

// GetFileModificationTime gets mtime of a file
func GetFileModificationTime(path string) time.Time {
	file, err := os.Stat(path)
	if err != nil {
		logger.Debug(module, "failed with %s", err)
		return time.Time{}
	}
	return file.ModTime()
}

// SetFileModificationTime sets mtime to a file
func SetFileModificationTime(path string, mt time.Time) {
	err := os.Chtimes(path, mt, mt)
	if err != nil {
		logger.Debug(module, "failed with %s", err)
	}
}

// GetWorkDir returns the directory where the executable file put
func GetWorkDir() string {
	ex, err := os.Executable()
	if err != nil {
		logger.Debug(module, "failed with %s", err)
	}
	path := filepath.Dir(ex)
	return path
}

// Chmod changes mod of a path
func Chmod(path string, mod os.FileMode) {
	err := os.Chmod(path, mod)
	if err != nil {
		logger.Debug(module, "failed with %s", path)
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
		logger.Debug(module, "failed with %s", path)
		return false
	}
	return fi.IsDir()
}

// IsPathFile determines if a path is a file
func IsPathFile(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		logger.Debug(module, "failed with %s", path)
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
		logger.Debug(module, "failed with %s", path)
		return 0
	}
	return fi.Size()
}

// GenTempFileName generate /tmp/%x files where %x is md5 value of all parts concate together
func GenTempFileName(parts ...string) string {
	var buf bytes.Buffer
	for _, part := range parts {
		buf.WriteString(part)
	}
	return fmt.Sprintf("/tmp/%x", md5.Sum(buf.Bytes()))

}

func readOrComputeCRC32c(path string) uint32 {
	result := uint32(0)
	cacheFileName := GenTempFileName(path, "-", GetFileModificationTime(path).String(), "-crc32c")

	b, e := os.ReadFile(cacheFileName)
	if e == nil {
		result = binary.LittleEndian.Uint32(b)
		logger.Debug(module, "loaded crc32c [%s] from catch: %d", cacheFileName, result)
		return result
	}
	file, err := os.Open(path)
	if err != nil {
		logger.Debug(module, "failed with %s", err)
		return 0
	}
	defer func() { _ = file.Close() }()
	crc32q := crc32.MakeTable(crc32.Castagnoli)
	h32 := crc32.New(crc32q)
	_, err = io.Copy(h32, file)
	if err != nil {
		logger.Debug(module, "failed with %s", err)
	}
	result = h32.Sum32()
	crcBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBytes, result)
	cf, _ := os.OpenFile(cacheFileName, os.O_WRONLY, 0766)
	defer func() {
		_ = cf.Close()
	}()
	if _, err = cf.Write(crcBytes); err != nil {
		logger.Debug(module, "write crc32c cachefile failed with %s", err)
	}
	if err = cf.Sync(); err != nil {
		logger.Debug(module, "write crc32c cachefile sync failed with %s", err)
	}
	if err == nil {
		logger.Debug(module, "wrote crc32c cachefile : %s", cacheFileName)
	}
	return result
}

// GetFileCRC32C gets the crc32c of a file
func GetFileCRC32C(path string) uint32 {
	path, _ = filepath.Abs(path)
	if IsPathDirectory(path) {
		return 0
	}
	return readOrComputeCRC32c(path)
}

// GetFileMD5 gets the md5 of a file
func GetFileMD5(path string) []byte {
	if IsPathDirectory(path) {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		logger.Debug(module, "failed with %s", err)
		return nil
	}
	defer func() { _ = file.Close() }()
	hash := md5.New()
	_, err = io.Copy(hash, file)
	if err != nil {
		logger.Debug(module, "failed with %s", err)
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
		logger.Debug(module, "failed with %s", err)
	}
}

// CreateFile creates an empty file with given length (if size > 0)
func CreateFile(path string, size int64) {
	f, err := os.Create(path)
	if err != nil {
		logger.Debug(module, "failed with %s", err)
		return
	}
	defer func() { _ = f.Close() }()
	if size > 0 {
		err = f.Truncate(size)
		if err != nil {
			logger.Debug(module, "failed with %s", err)
			return
		}
	}
}
