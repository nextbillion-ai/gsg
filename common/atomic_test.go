package common

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteFileAtomicWritesContentAndPerm(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache")
	assert.NoError(t, WriteFileAtomic(path, []byte("12345678"), 0644))

	b, err := os.ReadFile(path)
	assert.NoError(t, err)
	assert.Equal(t, []byte("12345678"), b)

	fi, err := os.Stat(path)
	assert.NoError(t, err)
	assert.Equal(t, os.FileMode(0), fi.Mode().Perm()&0022, "must not be group/world writable")
	assert.NotEqual(t, os.FileMode(0), fi.Mode().Perm()&0044, "must stay readable")
}

// Callers that want the file private get it; the mode is applied, not merely
// attempted.
func TestWriteFileAtomicHonoursPrivateMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lock")
	assert.NoError(t, WriteFileAtomic(path, []byte("12345678"), 0600))

	fi, err := os.Stat(path)
	assert.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), fi.Mode().Perm())
}

func TestWriteFileAtomicReplacesExisting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache")
	assert.NoError(t, WriteFileAtomic(path, []byte("old-and-longer"), 0644))
	assert.NoError(t, WriteFileAtomic(path, []byte("new"), 0644))

	b, err := os.ReadFile(path)
	assert.NoError(t, err)
	assert.Equal(t, []byte("new"), b, "no remnant of the longer previous content")
}

// The point of the helper: the file is never observable at a partial length,
// so a reader -- or a later run, after this one is killed -- cannot decode a
// short buffer and panic.
func TestWriteFileAtomicIsNeverPartiallyVisible(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cache")
	payload := []byte("12345678")

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
			if b, err := os.ReadFile(path); err == nil && len(b) != len(payload) {
				atomic.AddInt64(&shortReads, 1)
			}
		}
	}()

	for i := 0; i < 300; i++ {
		// Each acquisition writes a fresh cache file, so creation -- not
		// overwrite -- is the case that used to publish a zero-length file.
		_ = os.Remove(path)
		assert.NoError(t, WriteFileAtomic(path, payload, 0644))
	}
	close(stop)
	wg.Wait()

	assert.Equal(t, int64(0), atomic.LoadInt64(&shortReads))

	// No temp files left behind.
	entries, err := os.ReadDir(dir)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(entries))
}

func TestWriteFileAtomicErrorsOnMissingDir(t *testing.T) {
	assert.Error(t, WriteFileAtomic(filepath.Join(t.TempDir(), "nope", "cache"), []byte("x"), 0644))
}
