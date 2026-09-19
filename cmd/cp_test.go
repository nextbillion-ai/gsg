package cmd

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/nextbillion-ai/gsg/system"
	"github.com/nextbillion-ai/gsg/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errReset = errors.New("read tcp 127.0.0.1:54214->127.0.0.1:18080: read: connection reset by peer")

// flakySystem fails the first `failures` uploads and downloads it is asked for,
// the way one dropped connection does, and counts every attempt.
type flakySystem struct {
	system.ISystem
	failures int

	mu        sync.Mutex
	uploads   int
	downloads int
}

func (f *flakySystem) Scheme() string                        { return "gs" }
func (f *flakySystem) IsDirectory(_, _ string) (bool, error) { return false, nil }
func (f *flakySystem) IsObject(_, _ string) (bool, error)    { return true, nil }
func (f *flakySystem) Attributes(_, _ string) (*system.Attrs, error) {
	return &system.Attrs{Size: 1}, nil
}

func (f *flakySystem) Upload(_, _, _ string, _ system.RunContext) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.uploads++
	if f.uploads <= f.failures {
		return errReset
	}
	return nil
}

func (f *flakySystem) Download(_, _, _ string, _ bool, _ system.RunContext) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.downloads++
	if f.downloads <= f.failures {
		return errReset
	}
	return nil
}

func withPool(t *testing.T, workers int) {
	t.Helper()
	saved := pool
	pool = worker.New(workers, false)
	pool.Run()
	t.Cleanup(func() {
		pool.Close()
		pool = saved
	})
}

// cp sent each file once, so one dropped connection failed the command while
// rsync, which retries, went through.
func TestCpUploadTriesAgainAfterAFailedAttempt(t *testing.T) {
	withPool(t, 1)
	file := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o644))
	sys := &flakySystem{failures: 1}
	var wg sync.WaitGroup
	upload(system.ParseFileObject(file), &system.FileObject{System: sys, Bucket: "b", Prefix: "o", Remote: true}, false, false, &wg)
	wg.Wait()
	assert.Equal(t, 2, sys.uploads)
}

func TestCpDownloadTriesAgainAfterAFailedAttempt(t *testing.T) {
	withPool(t, 1)
	sys := &flakySystem{failures: 1}
	var wg sync.WaitGroup
	download(&system.FileObject{System: sys, Bucket: "b", Prefix: "o", Remote: true}, system.ParseFileObject(t.TempDir()), false, false, &wg)
	wg.Wait()
	assert.Equal(t, 2, sys.downloads)
}

// An object name that climbs out of the destination refuses the copy before
// anything is fetched, including the objects listed ahead of it.
func TestCpRefusesANameThatClimbsOutOfTheDestination(t *testing.T) {
	withPool(t, 2)
	sys := &fakeDownSystem{gather: 1, gathered: make(chan struct{}), names: []string{"a.sst", "../esc/evil.txt", "b.sst"}}
	var wg sync.WaitGroup
	download(&system.FileObject{System: sys, Bucket: "b", Prefix: "out/src", Remote: true}, system.ParseFileObject(t.TempDir()), false, true, &wg)
	wg.Wait()
	assert.Equal(t, 0, sys.maxSeen, "nothing was downloaded")
}
