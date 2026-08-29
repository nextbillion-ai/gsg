package gcs

import (
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nextbillion-ai/gsg/common"

	"github.com/stretchr/testify/assert"
)

func TestConfigPath(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "test_path")
	assert.Equal(t, "test_path", ConfigPath())
}

/*
func TestEuqalCRC32C(t *testing.T) {
	g := GCS{}

	assert.True(t, g.equalCRC32C("invalid", "invalid", "invalid"))
	assert.False(t, g.equalCRC32C("gcs.go", "invalid", "invalid"))
	// assert.True(t, equalCRC32C("usa.geojson", "maaas", "borders/usa.geojson"))
	// assert.False(t, equalCRC32C("invalid", "maaas", "borders/usa.geojson"))
}
*/

// A lock cache left short by a run that died mid-write used to panic here with
// "index out of range" when decoded as a uint64. There is no generation to
// match in a short file, so there is nothing it could unlock.
func TestAttemptUnLockIgnoresTruncatedCache(t *testing.T) {
	g := &GCS{}
	const bucket, object = "gsg-test-bucket", "gsg-test/lock-truncated"
	cacheFileName := common.GenTempFileName(bucket, "/", object)
	t.Cleanup(func() { _ = os.Remove(cacheFileName) })

	for _, short := range [][]byte{{}, {0x01}, {0x01, 0x02, 0x03, 0x04}, {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}} {
		assert.NoError(t, os.WriteFile(cacheFileName, short, 0600))
		assert.NotPanics(t, func() {
			// The remote lock stands until its TTL expires, so this must not
			// report a successful unlock.
			assert.Error(t, g.AttemptUnLock(bucket, object), "cache of %d byte(s)", len(short))
		}, "cache of %d byte(s)", len(short))

		// Left in place: another process may have just renamed a valid cache
		// over this path, and the next successful lock replaces it atomically.
		_, err := os.Stat(cacheFileName)
		assert.NoError(t, err, "invalid cache of %d byte(s) must not be removed", len(short))
	}
}

// With no cache at all there is likewise nothing to unlock.
func TestAttemptUnLockWithoutCache(t *testing.T) {
	g := &GCS{}
	const bucket, object = "gsg-test-bucket", "gsg-test/lock-absent"
	_ = os.Remove(common.GenTempFileName(bucket, "/", object))

	assert.NotPanics(t, func() {
		assert.NoError(t, g.AttemptUnLock(bucket, object))
	})
}

// crc32cToSend has to describe the bytes that will actually be uploaded, not
// whatever the path happens to name by then.
//
// It reads the handle for that reason. Consulting the cached checksum for the
// path would be cheaper -- the cache is keyed on path and mtime -- but it is
// then only as good as the assumption that content and mtime move together,
// and GCS rejects the object when they have not. The price of being wrong is a
// whole upload spent to be told so, which for a large file dwarfs the hash.
func TestCRC32CToSendFollowsTheOpenFileNotThePath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "moving.txt")
	original := []byte("the bytes that were opened\n")
	assert.NoError(t, os.WriteFile(path, original, 0600))

	f, err := os.Open(path)
	assert.NoError(t, err)
	defer func() { _ = f.Close() }()

	// Replace the path with different contents, as an atomic writer would.
	replacement := filepath.Join(dir, "replacement.txt")
	assert.NoError(t, os.WriteFile(replacement, []byte("completely different bytes\n"), 0600))
	assert.NoError(t, os.Rename(replacement, path))

	crc, err := crc32cToSend(f, path)
	assert.NoError(t, err)
	assert.Equal(t, crc32.Checksum(original, crc32.MakeTable(crc32.Castagnoli)), crc,
		"the checksum must describe the opened bytes, not the ones now at that path")

	// And the handle must be back at the start, or the upload sends a
	// truncated body that then fails the very check this is for.
	body, err := io.ReadAll(f)
	assert.NoError(t, err)
	assert.Equal(t, original, body)
}

// The ordinary case, where nothing has moved.
func TestCRC32CToSendMatchesTheFileWhenNothingMoved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stable.txt")
	content := []byte("nothing moved here\n")
	assert.NoError(t, os.WriteFile(path, content, 0600))

	f, err := os.Open(path)
	assert.NoError(t, err)
	defer func() { _ = f.Close() }()

	crc, err := crc32cToSend(f, path)
	assert.NoError(t, err)
	assert.Equal(t, crc32.Checksum(content, crc32.MakeTable(crc32.Castagnoli)), crc)

	body, err := io.ReadAll(f)
	assert.NoError(t, err)
	assert.Equal(t, content, body, "the handle is left where the upload needs it")
}

// A file whose contents change while its modification time is put back is what
// makes a path-and-mtime cache lie. Reading the handle is unaffected by it,
// which is the whole reason for reading the handle.
func TestCRC32CToSendIgnoresAStaleModificationTime(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sneaky.txt")
	assert.NoError(t, os.WriteFile(path, []byte("before\n"), 0600))

	fi, err := os.Stat(path)
	assert.NoError(t, err)
	mt := fi.ModTime()

	changed := []byte("afterx\n")
	assert.NoError(t, os.WriteFile(path, changed, 0600))
	assert.NoError(t, os.Chtimes(path, time.Now(), mt))

	f, err := os.Open(path)
	assert.NoError(t, err)
	defer func() { _ = f.Close() }()

	crc, err := crc32cToSend(f, path)
	assert.NoError(t, err)
	assert.Equal(t, crc32.Checksum(changed, crc32.MakeTable(crc32.Castagnoli)), crc,
		"the checksum must describe the current bytes, whatever the mtime says")
}
