package common

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
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
	return genCacheFileName(path, "-", GetFileModificationTime(path).String(), "-crc32c")
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

// setCacheDir points the crc32c cache at dir for one test, and makes the next
// resolution re-read the environment on both sides of it -- cacheDir memoizes,
// so a stale value would leak between tests.
func setCacheDir(t *testing.T, dir string) {
	t.Helper()
	t.Setenv(cacheDirEnv, dir)
	resetCacheDir()
	t.Cleanup(resetCacheDir)
}

// The crc32c cache defaults to /tmp, so one written by a build that predates
// GSG_CACHE_DIR is still the one this build reads.
func TestGenCacheFileNameDefaultsToTmp(t *testing.T) {
	setCacheDir(t, "")
	name := genCacheFileName("gs://bucket", "/", "object")
	assert.Equal(t, defaultCacheDir, filepath.Dir(name))
	assert.Equal(t, fmt.Sprintf("%x", md5.Sum([]byte("gs://bucket/object"))), filepath.Base(name))
}

// The point of the env var: the cache lands on a caller-chosen disk, and the
// name under it is the same one /tmp would have carried.
func TestGenCacheFileNameHonoursCacheDir(t *testing.T) {
	dir := t.TempDir()
	setCacheDir(t, "")
	want := filepath.Base(genCacheFileName("gs://bucket", "/", "object"))

	setCacheDir(t, dir)
	got := genCacheFileName("gs://bucket", "/", "object")
	assert.Equal(t, dir, filepath.Dir(got))
	assert.Equal(t, want, filepath.Base(got))
}

// The lock generation caches must NOT follow GSG_CACHE_DIR. Their name comes
// from the locked object alone, so two processes sharing a directory would
// share one file: the second to lock would overwrite the generation the first
// holds, and the first could then delete the second's lock. Keeping them
// process-local is what makes a stale generation fail GenerationMatch.
func TestGenTempFileNameIgnoresCacheDir(t *testing.T) {
	setCacheDir(t, t.TempDir())
	assert.Equal(t, defaultCacheDir, filepath.Dir(GenTempFileName("gs://bucket", "/", "lock")))
}

// The two namers agree on everything but the directory, so relocating the
// crc32c cache cannot change which entry a given input maps to.
func TestCacheFileNamesShareTheirHash(t *testing.T) {
	setCacheDir(t, t.TempDir())
	assert.Equal(t,
		filepath.Base(GenTempFileName("a", "b")),
		filepath.Base(genCacheFileName("a", "b")),
	)
}

// A cache directory that does not exist yet is created, otherwise every write
// into it would fail and the cache would silently never work.
func TestGenCacheFileNameCreatesCacheDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "cache")
	setCacheDir(t, dir)

	name := genCacheFileName("anything")
	assert.Equal(t, dir, filepath.Dir(name))
	fi, err := os.Stat(dir)
	assert.NoError(t, err)
	assert.True(t, fi.IsDir())

	// And it is usable: the whole point is that writeCRC32cCache can rename
	// into it.
	writeCRC32cCache(name, 42)
	got, ok := readCRC32cCache(name)
	assert.True(t, ok)
	assert.Equal(t, uint32(42), got)
}

// A GSG_CACHE_DIR that cannot be created must not take the cache down with it:
// falling back to /tmp is no worse than never setting the variable.
func TestGenCacheFileNameFallsBackWhenCacheDirCannotBeCreated(t *testing.T) {
	// A regular file cannot become a directory, so MkdirAll on a path under
	// it always fails.
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	assert.NoError(t, os.WriteFile(blocker, []byte("x"), 0644))
	setCacheDir(t, filepath.Join(blocker, "cache"))

	assert.Equal(t, defaultCacheDir, filepath.Dir(genCacheFileName("anything")))
}

// MkdirAll reports success for a directory that already exists whatever its
// mode, so an unwritable one used to be selected and then fail every write at
// Debug level, rehashing every file with nothing saying why.
func TestGenCacheFileNameFallsBackWhenCacheDirIsNotWritable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write a 0555 directory, so there is nothing to detect")
	}
	dir := filepath.Join(t.TempDir(), "readonly")
	assert.NoError(t, os.Mkdir(dir, 0555))
	setCacheDir(t, dir)

	assert.Equal(t, defaultCacheDir, filepath.Dir(genCacheFileName("anything")))
}

// A relative GSG_CACHE_DIR has to be pinned to an absolute path at resolution
// time. The result is memoized, so keeping the relative string would let a
// later chdir point the same stored value at a different directory, losing
// every entry written before it.
func TestResolveCacheDirAbsolutizesRelativePaths(t *testing.T) {
	base := t.TempDir()
	cwd, err := os.Getwd()
	assert.NoError(t, err)
	assert.NoError(t, os.Chdir(base))
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	setCacheDir(t, "relative-cache")
	got := cacheDir()
	assert.True(t, filepath.IsAbs(got), "cache dir should be absolute, got %q", got)

	// It must name the directory that was current when it resolved, not
	// whatever is current later.
	resolved, err := filepath.EvalSymlinks(got)
	assert.NoError(t, err)
	want, err := filepath.EvalSymlinks(filepath.Join(base, "relative-cache"))
	assert.NoError(t, err)
	assert.Equal(t, want, resolved)

	assert.NoError(t, os.Chdir(cwd))
	assert.Equal(t, got, cacheDir())
}

// The probe must not leave anything behind: it runs once per process, but a
// leaked probe file in a persisted directory would accumulate forever.
func TestResolveCacheDirLeavesNoProbeFile(t *testing.T) {
	dir := t.TempDir()
	setCacheDir(t, dir)

	assert.Equal(t, dir, cacheDir())
	entries, err := os.ReadDir(dir)
	assert.NoError(t, err)
	assert.Empty(t, entries)
}

// The end-to-end property nbroute needs: a checksum computed once is persisted
// into the configured directory, and a later read comes back from there rather
// than from the file.
func TestGetFileCRC32CPersistsIntoCacheDir(t *testing.T) {
	dir := t.TempDir()
	setCacheDir(t, dir)

	path := filepath.Join(t.TempDir(), "data")
	assert.NoError(t, os.WriteFile(path, []byte("hello world"), 0644))
	want := GetFileCRC32C(path)

	// GetFileCRC32C itself must have written the entry -- the half the test
	// used to skip by writing the sentinel before ever checking. Without this
	// the test would still pass if nothing were ever persisted.
	cachePath := crc32cCachePath(t, path)
	assert.Equal(t, dir, filepath.Dir(cachePath))
	b, err := os.ReadFile(cachePath)
	assert.NoError(t, err)
	assert.Equal(t, crc32cCacheSize, len(b))
	assert.Equal(t, want, binary.LittleEndian.Uint32(b))

	// And it is those bytes that come back, not a fresh computation.
	writeCRC32cCache(cachePath, want+1)
	assert.Equal(t, want+1, GetFileCRC32C(path))
}
