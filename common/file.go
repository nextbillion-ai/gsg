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
	"sync"
	"time"

	"github.com/nextbillion-ai/gsg/logger"
)

const (
	tempFileSuffix = "_.gstmp"
	// crc32cCacheSize is the exact byte length of a crc32c cache file.
	crc32cCacheSize = 4
	// crc32cCachePerm keeps the cache readable by other users sharing the
	// cache directory.
	crc32cCachePerm = 0644
	// defaultCacheDir is where every cache file has always lived.
	defaultCacheDir = "/tmp"
	// cacheDirEnv names a directory to hold the crc32c cache instead of
	// defaultCacheDir.
	//
	// That cache is keyed by path and mtime, and Download stamps a downloaded
	// file with the remote object's mtime, so an entry stays valid for as long
	// as the object does -- across processes, and across the life of whatever
	// wrote it. In a container /tmp is the ephemeral layer, which throws that
	// away on every restart and is not even shared between two containers of
	// the same pod, so a caller that has a persistent disk had no way to keep
	// a cache that is designed to outlive a single run. Point this at
	// somewhere on that disk and it does.
	//
	// It moves ONLY the crc32c cache. The lock generation caches keep using
	// GenTempFileName and stay in /tmp on purpose: their name is derived from
	// the locked object alone, so two processes sharing a directory share one
	// file, and the second to lock overwrites the generation the first is
	// holding -- after which the first can unlock the second's lock. Two
	// processes in one container can already collide that way, since /tmp is
	// container-local rather than process-local; the point is not to widen it
	// to every process that mounts the same volume.
	cacheDirEnv = "GSG_CACHE_DIR"
	// cacheDirPerm matches the 0755 CreateFolder uses. The files inside carry
	// their own modes; 0700 would stop the sharing crc32cCachePerm allows.
	cacheDirPerm = 0755
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

// hashParts is the %x half of a cache file name: the md5 of all parts
// concatenated. Unchanged from when every name was built as /tmp/%x, so a
// cache written by an older gsg is still found.
func hashParts(parts []string) string {
	var buf bytes.Buffer
	for _, part := range parts {
		buf.WriteString(part)
	}
	return fmt.Sprintf("%x", md5.Sum(buf.Bytes()))
}

// GenTempFileName generates /tmp/%x files where %x is md5 value of all parts
// concate together. Used for the lock generation caches, which must stay
// process-local -- see cacheDirEnv.
func GenTempFileName(parts ...string) string {
	return filepath.Join(defaultCacheDir, hashParts(parts))
}

// genCacheFileName is GenTempFileName for files that may be shared and kept:
// the crc32c cache, under cacheDir rather than always /tmp.
func genCacheFileName(parts ...string) string {
	return filepath.Join(cacheDir(), hashParts(parts))
}

var (
	cacheDirOnce sync.Once
	cacheDirPath string
)

// cacheDir is the directory the crc32c cache lives in: defaultCacheDir, or
// whatever cacheDirEnv names.
//
// Resolved once per process. It has to be stable: the read side has to agree
// with the write side on where an entry is, and re-deciding per call would let
// a directory that appears or disappears mid-run split the cache in two.
func cacheDir() string {
	cacheDirOnce.Do(func() { cacheDirPath = resolveCacheDir() })
	return cacheDirPath
}

// resetCacheDir makes the next cacheDir call re-read the environment. Tests
// only -- nothing in a real run changes cacheDirEnv mid-process.
func resetCacheDir() {
	cacheDirOnce = sync.Once{}
	cacheDirPath = ""
}

// resolveCacheDir picks the directory, falling back to defaultCacheDir when
// the configured one cannot actually be used. A misconfigured GSG_CACHE_DIR
// then costs the persistence it was set to gain, but never more than not
// setting it at all, which is the safe direction for a cache.
//
// The directory is created here rather than at the write, because the read
// side has to agree on the name before anything has been written.
//
// Note that a directory shared by several processes is only safe when they all
// see the same filesystem at the same paths -- as two containers mounting one
// volume do. The crc32c key is a path and an mtime, so processes that mean
// different files by the same path would read each other's checksums.
func resolveCacheDir() string {
	dir := os.Getenv(cacheDirEnv)
	if dir == "" {
		return defaultCacheDir
	}
	// Resolved against the working directory once, here. The result is
	// memoized, so holding on to a relative path would mean a later chdir
	// silently moved the cache: the same stored string, a different directory,
	// and entries written before the chdir no longer found.
	abs, err := filepath.Abs(dir)
	if err != nil {
		logger.Info(module, "%s is [%s], which cannot be resolved to an absolute path (%s); falling back to %s", cacheDirEnv, dir, err, defaultCacheDir)
		return defaultCacheDir
	}
	dir = abs
	if err := os.MkdirAll(dir, cacheDirPerm); err != nil {
		logger.Info(module, "%s is [%s], which cannot be created (%s); falling back to %s", cacheDirEnv, dir, err, defaultCacheDir)
		return defaultCacheDir
	}
	// MkdirAll reports success for a directory that already exists, whatever
	// its mode or owner. Selecting a read-only mount, or one belonging to
	// another uid, would leave every write failing at Debug level and every
	// file rehashed with nothing saying why -- so prove it is writable rather
	// than assume it.
	probe, err := os.CreateTemp(dir, ".gsg-probe")
	if err != nil {
		logger.Info(module, "%s is [%s], which is not writable (%s); falling back to %s", cacheDirEnv, dir, err, defaultCacheDir)
		return defaultCacheDir
	}
	name := probe.Name()
	_ = probe.Close()
	_ = os.Remove(name)
	return dir
}

func readOrComputeCRC32c(path string) uint32 {
	result := uint32(0)
	cacheFileName := genCacheFileName(path, "-", GetFileModificationTime(path).String(), "-crc32c")

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
			// touch on hit, so an mtime sweep of the cache dir measures time since last use
			now := time.Now()
			if err := os.Chtimes(cacheFileName, now, now); err != nil {
				logger.Debug(module, "touch crc32c cachefile [%s] failed with %s", cacheFileName, err)
			}
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
// staying readable by another sharing the cache directory saves real work, and
// there is nothing sensitive in it.
func writeCRC32cCache(cacheFileName string, result uint32) {
	crcBytes := make([]byte, crc32cCacheSize)
	binary.LittleEndian.PutUint32(crcBytes, result)

	if err := WriteFileAtomic(cacheFileName, crcBytes, crc32cCachePerm); err != nil {
		logger.Debug(module, "write crc32c cachefile [%s] failed with %s", cacheFileName, err)
		return
	}
	logger.Debug(module, "wrote crc32c cachefile : %s", cacheFileName)
}

// StoreFileCRC32C records a crc32c that was computed over the file's content
// while it was being written, so that GetFileCRC32C does not read the file to
// learn it. Call it once the file has its final name and modification time:
// both are part of the key.
func StoreFileCRC32C(path string, crc uint32) {
	path, _ = filepath.Abs(path)
	writeCRC32cCache(genCacheFileName(path, "-", GetFileModificationTime(path).String(), "-crc32c"), crc)
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
