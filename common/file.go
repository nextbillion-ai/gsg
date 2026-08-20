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
	// crc32cCacheSize is the exact byte length of a crc32c cache file.
	crc32cCacheSize = 4
)

var (
	// GentleIO controls whether to use gentle I/O for CRC calculation
	// When true, uses fadviseDontNeed and throttling to reduce cache pollution
	GentleIO = false
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

	if cached, ok := readCRC32cCache(cacheFileName); ok {
		logger.Debug(module, "loaded crc32c [%s] from catch: %d", cacheFileName, cached)
		return cached
	}

	logger.Debug(module, "Computing CRC32C for [%s], size: %d bytes, gentle mode: %t", path, GetFileSize(path), GentleIO)
	file, err := os.Open(path)
	if err != nil {
		logger.Debug(module, "failed with %s", err)
		return 0
	}
	defer func() { _ = file.Close() }()

	crc32q := crc32.MakeTable(crc32.Castagnoli)
	h32 := crc32.New(crc32q)

	// A sum over a partially read file is wrong; it must never be cached.
	complete := true
	if GentleIO {
		// Gentle mode: use fadvise and throttling to reduce impact on other processes
		fadviseSequential(file)

		const bufSize = 10 * 1024 * 1024 // 10MB
		buf := make([]byte, bufSize)
		totalRead := int64(0)

		for {
			n, readErr := file.Read(buf)
			if n > 0 {
				if _, writeErr := h32.Write(buf[:n]); writeErr != nil {
					logger.Debug(module, "failed to write to hash: %s", writeErr)
					complete = false
					break
				}

				// Tell kernel to drop this chunk from cache
				fadviseDontNeed(file, totalRead, int64(n))
				totalRead += int64(n)

				// Yield to other I/O operations
				time.Sleep(time.Millisecond * 5)
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				logger.Debug(module, "failed with %s", readErr)
				complete = false
				break
			}
		}
	} else {
		// Fast mode: standard io.Copy without throttling
		_, err = io.Copy(h32, file)
		if err != nil {
			logger.Debug(module, "failed with %s", err)
			complete = false
		}
	}

	result = h32.Sum32()
	logger.Debug(module, "Computed CRC32C for [%s]: %d", path, result)
	if !complete {
		// Return the sum anyway -- callers treat a CRC mismatch as a failed
		// transfer, which is the safe direction -- but caching it would make a
		// transient read error permanent for this path and mtime.
		logger.Debug(module, "not caching crc32c for [%s]: file was not read in full", path)
		return result
	}
	writeCRC32cCache(cacheFileName, result)
	return result
}

// readCRC32cCache returns the cached crc32c for cacheFileName, reporting false
// when there is no usable cache. Anything that is not exactly crc32cCacheSize
// bytes of regular file was left behind by a run that died mid-write, so it is
// removed rather than decoded -- reading it as a uint32 used to panic with
// "index out of range [3] with length 0". The size is checked before any bytes
// are read, so a stray huge file under the cache name cannot be slurped into
// memory.
func readCRC32cCache(cacheFileName string) (uint32, bool) {
	cf, err := os.Open(cacheFileName)
	if err != nil {
		return 0, false
	}
	defer func() { _ = cf.Close() }()

	fi, err := cf.Stat()
	if err != nil {
		logger.Debug(module, "stat crc32c cachefile [%s] failed with %s", cacheFileName, err)
		return 0, false
	}
	if fi.Mode().IsRegular() && fi.Size() == crc32cCacheSize {
		b := make([]byte, crc32cCacheSize)
		if _, err = io.ReadFull(cf, b); err == nil {
			return binary.LittleEndian.Uint32(b), true
		}
		logger.Debug(module, "read crc32c cachefile [%s] failed with %s", cacheFileName, err)
	}

	logger.Debug(module, "discarding unusable crc32c cachefile [%s] of %d byte(s)", cacheFileName, fi.Size())
	if err = os.Remove(cacheFileName); err != nil {
		logger.Debug(module, "failed to remove unusable crc32c cachefile with %s", err)
	}
	return 0, false
}

// writeCRC32cCache persists a crc32c value to the cache file atomically.
// The bytes go to a temp file that is renamed into place, so neither a
// concurrent reader nor a later run can observe a half-written cache file --
// opening the cache path directly published a zero-length file before the
// value landed, and anything that killed the process in between (a failing
// object calling common.Exit, a signal) left that empty file behind for every
// subsequent run to trip over.
func writeCRC32cCache(cacheFileName string, result uint32) {
	crcBytes := make([]byte, crc32cCacheSize)
	binary.LittleEndian.PutUint32(crcBytes, result)

	cf, err := os.CreateTemp(filepath.Dir(cacheFileName), filepath.Base(cacheFileName)+".tmp")
	if err != nil {
		logger.Debug(module, "open crc32c cachefile failed with %s", err)
		return
	}
	tempName := cf.Name()
	defer func() {
		_ = cf.Close()
		// A no-op once the rename below succeeded.
		_ = os.Remove(tempName)
	}()

	// CreateTemp uses 0600. Keep the cache readable by other users sharing /tmp,
	// as it has always been, but not writable by them -- the old 0766 was
	// filtered by umask to 0744, so setting 0766 outright would widen it.
	if err = cf.Chmod(0644); err != nil {
		logger.Debug(module, "chmod crc32c cachefile failed with %s", err)
	}
	if _, err = cf.Write(crcBytes); err != nil {
		logger.Debug(module, "write crc32c cachefile failed with %s", err)
		return
	}
	if err = cf.Sync(); err != nil {
		logger.Debug(module, "write crc32c cachefile sync failed with %s", err)
		return
	}
	if err = cf.Close(); err != nil {
		logger.Debug(module, "close crc32c cachefile failed with %s", err)
		return
	}
	if err = os.Rename(tempName, cacheFileName); err != nil {
		logger.Debug(module, "rename crc32c cachefile failed with %s", err)
		return
	}
	logger.Debug(module, "wrote crc32c cachefile : %s", cacheFileName)
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
