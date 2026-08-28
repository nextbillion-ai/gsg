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
	// crc32cCachePerm keeps the cache readable by other users sharing /tmp.
	crc32cCachePerm = 0644
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

func readOrComputeCRC32c(path string) (uint32, bool) {
	result := uint32(0)
	cacheFileName := GenTempFileName(path, "-", GetFileModificationTime(path).String(), "-crc32c")

	if cached, ok := readCRC32cCache(cacheFileName); ok {
		logger.Debug(module, "loaded crc32c [%s] from catch: %d", cacheFileName, cached)
		return cached, true
	}

	logger.Debug(module, "Computing CRC32C for [%s], size: %d bytes, gentle mode: %t", path, GetFileSize(path), GentleIO)
	file, err := os.Open(path)
	if err != nil {
		logger.Debug(module, "failed with %s", err)
		return 0, false
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
		// transient read error permanent for this path and mtime. The second
		// result is what lets a caller tell this apart from a real checksum,
		// which matters when the number is about to be sent to a service that
		// will reject the upload if it disagrees.
		logger.Debug(module, "not caching crc32c for [%s]: file was not read in full", path)
		return result, false
	}
	writeCRC32cCache(cacheFileName, result)
	return result, true
}

// readCRC32cCache returns the cached crc32c for cacheFileName, reporting false
// when there is no usable cache. Anything that is not exactly crc32cCacheSize
// bytes of regular file was left behind by a run that died mid-write, so it is
// ignored rather than decoded -- reading it as a uint32 used to panic with
// "index out of range [3] with length 0". The size is checked before any bytes
// are read, so a stray huge file under the cache name cannot be slurped into
// memory.
//
// An unusable file is left where it is. Deleting from the read path would race:
// another process may have renamed a good cache in between the read above and
// the delete, and we would throw that away. A bad regular file is replaced by
// the next complete computation anyway, since writeCRC32cCache renames over
// this path. A path that rename cannot replace -- a directory, say -- does
// survive, at the cost of recomputing this file's crc32c every time.
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

	logger.Debug(module, "ignoring unusable crc32c cachefile [%s] of %d byte(s)", cacheFileName, fi.Size())
	return 0, false
}

// writeCRC32cCache persists a crc32c value to the cache file atomically, so
// that neither a concurrent reader nor a later run can observe it half written.
//
// 0644 rather than the 0600 the lock caches use: a checksum cached by one user
// staying readable by another sharing /tmp saves real work, and there is
// nothing sensitive in it.
func writeCRC32cCache(cacheFileName string, result uint32) {
	crcBytes := make([]byte, crc32cCacheSize)
	binary.LittleEndian.PutUint32(crcBytes, result)

	if err := WriteFileAtomic(cacheFileName, crcBytes, crc32cCachePerm); err != nil {
		logger.Debug(module, "write crc32c cachefile [%s] failed with %s", cacheFileName, err)
		return
	}
	logger.Debug(module, "wrote crc32c cachefile : %s", cacheFileName)
}

// GetFileCRC32C gets the crc32c of a file.
//
// A zero means either that the checksum is zero or that it could not be
// computed, and there is no way to tell which -- see TODO item 4. Prefer
// GetFileCRC32CChecked where the difference matters.
func GetFileCRC32C(path string) uint32 {
	v, _ := GetFileCRC32CChecked(path)
	return v
}

// GetFileCRC32CChecked gets the crc32c of a file, and says whether it is a
// real checksum.
//
// The second result is false when the file could not be opened, could not be
// read to the end, or is not a file at all. Callers that merely compare
// checksums can ignore it: a wrong number reads as a mismatch, which is the
// safe direction. Callers that send the number to a service cannot, because a
// zero standing in for "unknown" would have a perfectly good upload rejected.
func GetFileCRC32CChecked(path string) (uint32, bool) {
	path, _ = filepath.Abs(path)
	if IsPathDirectory(path) {
		return 0, false
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
