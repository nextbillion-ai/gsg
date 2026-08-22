package common

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsPathExist(t *testing.T) {
	assert.True(t, IsPathExist(""))
	assert.True(t, IsPathExist("."))
	assert.True(t, IsPathExist("../cmd"))
	assert.False(t, IsPathExist("../invalid_who_cares"))
	assert.True(t, IsPathExist("./file_test.go"))
	assert.False(t, IsPathExist("./invalid_who_cares.go"))
}

func TestIsPathDirectory(t *testing.T) {
	assert.True(t, IsPathDirectory("."))
	assert.True(t, IsPathDirectory("../cmd"))
	assert.False(t, IsPathDirectory("../invalid_who_cares"))
	assert.False(t, IsPathDirectory("./file_test.go"))
	assert.False(t, IsPathDirectory("./invalid_who_cares.go"))
}

func TestIsPathFile(t *testing.T) {
	assert.False(t, IsPathFile("."))
	assert.False(t, IsPathFile("../cmd"))
	assert.False(t, IsPathFile("../invalid_who_cares"))
	assert.True(t, IsPathFile("./file_test.go"))
	assert.False(t, IsPathFile("./invalid_who_cares.go"))
}

func TestGetFileSize(t *testing.T) {
	assert.Less(t, int64(0), GetFileSize("file.go"))
	assert.Equal(t, int64(0), GetFileSize("invalid_who_cares.go"))
	assert.Equal(t, int64(0), GetFileSize("invalid_who_cares"))
	assert.Equal(t, int64(0), GetFileSize("."))
}

func TestGetFileMD5(t *testing.T) {
	assert.Less(t, 0, len(GetFileMD5("file.go")))
	assert.Equal(t, 0, len(GetFileMD5("invalid_who_cares.go")))
	assert.Equal(t, 0, len(GetFileMD5("invalid_who_cares")))
	assert.Equal(t, 0, len(GetFileMD5(".")))
}
func TestIsTempFile(t *testing.T) {
	assert.False(t, IsTempFile(""))
	assert.False(t, IsTempFile("file.go"))
	assert.True(t, IsTempFile("file.go_.gstmp"))
	assert.True(t, IsTempFile("abc/file.go_.gstmp"))
	assert.True(t, IsTempFile("/abc/file.go_.gstmp"))
	assert.True(t, IsTempFile("gs://abc/file.go_.gstmp"))
}

func TestGetTempFile(t *testing.T) {
	assert.Equal(t, "", GetTempFile(""))
	assert.Equal(t, "file.go_.gstmp", GetTempFile("file.go"))
	assert.Equal(t, "abc/file.go_.gstmp", GetTempFile("abc/file.go"))
	assert.Equal(t, "/abc/file.go_.gstmp", GetTempFile("/abc/file.go"))
	assert.Equal(t, "gs://abc/file.go_.gstmp", GetTempFile("gs://abc/file.go"))
}

// crc32cCachePath mirrors the cache name readOrComputeCRC32c derives for a path.
func crc32cCachePath(t *testing.T, path string) string {
	t.Helper()
	return GenTempFileName(path, "-", GetFileModificationTime(path).String(), "-crc32c")
}

// newCRC32cFixture writes a data file and returns it with its cache path,
// cleaning both up afterwards.
func newCRC32cFixture(t *testing.T) (path, cachePath string) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "data")
	assert.NoError(t, os.WriteFile(path, []byte("hello world"), 0644))
	cachePath = crc32cCachePath(t, path)
	t.Cleanup(func() { _ = os.Remove(cachePath) })
	return path, cachePath
}

// A cache file left truncated by a run that died mid-write used to panic with
// "index out of range [3] with length 0" instead of being recomputed.
func TestGetFileCRC32CIgnoresTruncatedCache(t *testing.T) {
	path, cachePath := newCRC32cFixture(t)
	want := GetFileCRC32C(path)
	assert.NotEqual(t, uint32(0), want)

	for _, corrupt := range [][]byte{{}, {0x01}, {0x01, 0x02}, {0x01, 0x02, 0x03}, {0x01, 0x02, 0x03, 0x04, 0x05}} {
		assert.NoError(t, os.WriteFile(cachePath, corrupt, 0766))
		assert.NotPanics(t, func() {
			assert.Equal(t, want, GetFileCRC32C(path))
		}, "cache of %d byte(s)", len(corrupt))
		// The corrupt cache is replaced, so it cannot poison later runs.
		b, err := os.ReadFile(cachePath)
		assert.NoError(t, err)
		assert.Equal(t, crc32cCacheSize, len(b))
	}
}

func TestGetFileCRC32CUsesCache(t *testing.T) {
	path, cachePath := newCRC32cFixture(t)
	want := GetFileCRC32C(path)

	b, err := os.ReadFile(cachePath)
	assert.NoError(t, err)
	assert.Equal(t, crc32cCacheSize, len(b))
	assert.Equal(t, want, binary.LittleEndian.Uint32(b))

	// Overwriting the cache with a different well-formed value proves the
	// cached bytes are what gets returned, rather than a fresh computation.
	sentinel := want + 1
	sentinelBytes := make([]byte, crc32cCacheSize)
	binary.LittleEndian.PutUint32(sentinelBytes, sentinel)
	assert.NoError(t, os.WriteFile(cachePath, sentinelBytes, 0766))
	assert.Equal(t, sentinel, GetFileCRC32C(path))
}

// The cache file must never be observable at any size other than 4 bytes,
// which is what makes a reader or a later run safe.
func TestWriteCRC32cCacheIsAtomic(t *testing.T) {
	dir := t.TempDir()
	cachePath := filepath.Join(dir, "cache")

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var shortReads int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if b, err := os.ReadFile(cachePath); err == nil && len(b) != crc32cCacheSize {
				atomic.AddInt64(&shortReads, 1)
			}
		}
	}()

	for i := 0; i < 300; i++ {
		// Every distinct path+mtime gets its own cache file, so a big sync
		// creates thousands of them. Creation, not overwrite, is the case that
		// used to publish a zero-length file.
		_ = os.Remove(cachePath)
		writeCRC32cCache(cachePath, uint32(i))
	}
	close(stop)
	wg.Wait()

	assert.Equal(t, int64(0), atomic.LoadInt64(&shortReads))
	// No temp files are left behind.
	entries, err := os.ReadDir(dir)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(entries))
}

// The cache lives in a world-writable /tmp, so it must not itself be writable
// by other users. The old code passed 0766 to os.OpenFile, where umask cut it
// down to 0744 -- writing 0766 outright would have widened it.
func TestCRC32cCacheIsNotWorldWritable(t *testing.T) {
	path, cachePath := newCRC32cFixture(t)
	GetFileCRC32C(path)

	fi, err := os.Stat(cachePath)
	assert.NoError(t, err)
	// The exact mode, not just "not writable and readable somehow": readable by
	// others sharing /tmp, writable by nobody else.
	assert.Equal(t, os.FileMode(crc32cCachePerm), fi.Mode().Perm())
}

// Anything at the cache path that is not a 4-byte regular file is discarded
// without being read, so a stray directory or huge file cannot break or stall
// the checksum path.
func TestGetFileCRC32CIgnoresNonRegularCache(t *testing.T) {
	path, cachePath := newCRC32cFixture(t)
	want := GetFileCRC32C(path)

	assert.NoError(t, os.Remove(cachePath))
	assert.NoError(t, os.Mkdir(cachePath, 0755))
	t.Cleanup(func() { _ = os.RemoveAll(cachePath) })

	assert.NotPanics(t, func() {
		assert.Equal(t, want, GetFileCRC32C(path))
	})
}
