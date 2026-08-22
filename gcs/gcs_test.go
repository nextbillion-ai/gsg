package gcs

import (
	"os"
	"testing"

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
