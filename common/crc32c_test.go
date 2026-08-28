package common

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// GetFileCRC32CChecked exists so a caller can tell a checksum of zero from a
// checksum it could not compute. GetFileCRC32C returns 0 for both, which is
// fine for comparing -- a wrong number reads as a mismatch -- but not for
// sending to a service that will reject the upload if it disagrees.
func TestGetFileCRC32CChecked(t *testing.T) {
	dir := t.TempDir()

	content := filepath.Join(dir, "content.txt")
	assert.NoError(t, os.WriteFile(content, []byte("some bytes\n"), 0600))
	v, ok := GetFileCRC32CChecked(content)
	assert.True(t, ok, "a readable file has a checksum")
	assert.NotZero(t, v)
	assert.Equal(t, v, GetFileCRC32C(content), "both spellings agree on the value")

	// An empty file's checksum really is zero, and that is a real answer --
	// the distinction this function exists to make.
	empty := filepath.Join(dir, "empty.txt")
	assert.NoError(t, os.WriteFile(empty, nil, 0600))
	v, ok = GetFileCRC32CChecked(empty)
	assert.True(t, ok, "an empty file still has a checksum")
	assert.Equal(t, uint32(0), v)

	// These are the cases that must not look like a checksum of zero.
	for _, c := range []struct{ path, why string }{
		{filepath.Join(dir, "absent.txt"), "a file that is not there"},
		{dir, "a directory"},
	} {
		v, ok = GetFileCRC32CChecked(c.path)
		assert.False(t, ok, "%s should report that it has no checksum", c.why)
		assert.Equal(t, uint32(0), v)
	}
}

// An unreadable file must not pass as a checksum of zero: the gs upload sends
// this value, and a zero standing in for "unknown" would have the service
// reject a file that is probably fine.
func TestGetFileCRC32CCheckedOnAnUnreadableFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root, which can read a file with no permissions")
	}
	dir := t.TempDir()
	locked := filepath.Join(dir, "locked.txt")
	assert.NoError(t, os.WriteFile(locked, []byte("secret"), 0600))
	assert.NoError(t, os.Chmod(locked, 0000))
	defer func() { _ = os.Chmod(locked, 0600) }()

	v, ok := GetFileCRC32CChecked(locked)
	assert.False(t, ok, "a file that cannot be opened has no checksum to report")
	assert.Equal(t, uint32(0), v)
}
