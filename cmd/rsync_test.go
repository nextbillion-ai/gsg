package cmd

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nextbillion-ai/gsg/system"
	"github.com/nextbillion-ai/gsg/worker"
	"github.com/stretchr/testify/assert"
)

// fakeDownSystem lists a flat directory of small objects and records how many
// of its downloads were in flight at once.
type fakeDownSystem struct {
	system.ISystem
	names []string
	// a download holds until this many are in flight, or until it gives up
	gather int

	mu       sync.Mutex
	inFlight int
	maxSeen  int
	gathered chan struct{}
	once     sync.Once
}

func (f *fakeDownSystem) Scheme() string                        { return "gs" }
func (f *fakeDownSystem) IsDirectory(_, _ string) (bool, error) { return true, nil }
func (f *fakeDownSystem) IsObject(_, _ string) (bool, error)    { return false, nil }

func (f *fakeDownSystem) List(bucket, prefix string, _ bool) ([]*system.FileObject, error) {
	fos := make([]*system.FileObject, 0, len(f.names))
	for _, name := range f.names {
		fo := &system.FileObject{System: f, Bucket: bucket, Prefix: prefix + "/" + name, Remote: true}
		fo.SetAttributes(&system.Attrs{Size: 1, ModTime: time.Unix(1700000000, 0)})
		fos = append(fos, fo)
	}
	return fos, nil
}

func (f *fakeDownSystem) Download(_, _, _ string, _ bool, _ system.RunContext) error {
	f.mu.Lock()
	f.inFlight++
	if f.inFlight > f.maxSeen {
		f.maxSeen = f.inFlight
	}
	if f.inFlight >= f.gather {
		f.once.Do(func() { close(f.gathered) })
	}
	f.mu.Unlock()

	select {
	case <-f.gathered:
	case <-time.After(300 * time.Millisecond):
	}

	f.mu.Lock()
	f.inFlight--
	f.mu.Unlock()
	return nil
}

func runDownsync(t *testing.T, workers, files, gather int) *fakeDownSystem {
	t.Helper()
	sys := &fakeDownSystem{gather: gather, gathered: make(chan struct{})}
	for i := 0; i < files; i++ {
		sys.names = append(sys.names, fmt.Sprintf("%06d.sst", i))
	}
	saved := pool
	defer func() { pool = saved }()
	pool = worker.New(workers, false)
	pool.Run()

	src := &system.FileObject{System: sys, Bucket: "bucket", Prefix: "out/cache", Remote: true}
	dst := system.ParseFileObject(t.TempDir())
	downsync(src, dst, true, false, false)
	pool.Close()
	return sys
}

// A directory of many small objects is one chunk per object, so the chunk
// workers cannot make it parallel: the objects themselves have to be.
func TestDownsyncDownloadsObjectsInParallel(t *testing.T) {
	sys := runDownsync(t, 8, 8, 8)
	assert.Equal(t, 8, sys.maxSeen)
}

// Without -m the pool has one worker, which is what keeps a gentle sync to one
// object at a time.
func TestDownsyncStaysSequentialWithOneWorker(t *testing.T) {
	sys := runDownsync(t, 1, 3, 2)
	assert.Equal(t, 1, sys.maxSeen)
}
