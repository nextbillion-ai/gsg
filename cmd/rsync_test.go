package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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

// An object name that climbs out of the destination refuses the sync before
// anything is fetched, including the objects listed ahead of it.
func TestDownsyncRefusesANameThatClimbsOutOfTheDestination(t *testing.T) {
	sys := &fakeDownSystem{gather: 1, gathered: make(chan struct{}), names: []string{"a.sst", "../esc/evil.txt", "b.sst"}}
	saved := pool
	defer func() { pool = saved }()
	pool = worker.New(2, false)
	pool.Run()
	src := &system.FileObject{System: sys, Bucket: "bucket", Prefix: "out/cache", Remote: true}
	downsync(src, system.ParseFileObject(t.TempDir()), true, false, false)
	pool.Close() // waits for anything that was queued
	assert.Equal(t, 0, sys.maxSeen, "nothing was downloaded")
}

func objectsAt(modTime time.Time, noChecksum bool) map[string]*system.FileObject {
	attrs := &system.Attrs{RelativePath: "a.bin", Size: 1, ModTime: modTime,
		CalcCRC32C: func() (uint32, bool) { return 0, false },
		NoCRC32C:   func() bool { return noChecksum }}
	fo := &system.FileObject{Prefix: "a.bin", Remote: true}
	fo.SetAttributes(attrs)
	return map[string]*system.FileObject{"a.bin": fo}
}

func localAt(modTime time.Time) map[string]*system.FileObject {
	fo := &system.FileObject{Prefix: "a.bin"}
	fo.SetAttributes(&system.Attrs{RelativePath: "a.bin", Size: 1, ModTime: modTime,
		CalcCRC32C: func() (uint32, bool) { return 1, true }})
	return map[string]*system.FileObject{"a.bin": fo}
}

func TestDownloadDiffsSkipsAnUnchangedObjectWithNoChecksum(t *testing.T) {
	at := time.Unix(1700000000, 0)
	copyList, deleteList := downloadDiffs(objectsAt(at, true), localAt(at), true)
	assert.Empty(t, copyList)
	assert.Empty(t, deleteList)
}

func TestDownloadDiffsFetchesAnObjectWithNoChecksumAtANewTime(t *testing.T) {
	at := time.Unix(1700000000, 0)
	copyList, _ := downloadDiffs(objectsAt(at.Add(time.Second), true), localAt(at), true)
	assert.Len(t, copyList, 1)
}

func TestDownloadDiffsFetchesAfterAFailedLookup(t *testing.T) {
	at := time.Unix(1700000000, 0)
	copyList, _ := downloadDiffs(objectsAt(at, false), localAt(at), true)
	assert.Len(t, copyList, 1)
}

func TestDownloadDiffsFetchesAMissingFileAndDeletesAnExtraOne(t *testing.T) {
	at := time.Unix(1700000000, 0)
	copyList, deleteList := downloadDiffs(objectsAt(at, true), map[string]*system.FileObject{}, true)
	assert.Len(t, copyList, 1)
	assert.Empty(t, deleteList)

	copyList, deleteList = downloadDiffs(map[string]*system.FileObject{}, localAt(at), true)
	assert.Empty(t, copyList)
	assert.Len(t, deleteList, 1)
}

// Only a download may use the time. In an upload or a cloud-to-cloud copy the
// same pair stays a difference, because the destination's time is its own.
func TestPlainDiffsStillCopyAnObjectWithNoChecksum(t *testing.T) {
	at := time.Unix(1700000000, 0)
	copyList, _ := diffs(objectsAt(at, true), localAt(at), true)
	assert.Len(t, copyList, 1)
	copyList, _ = diffs(objectsAt(at, true), objectsAt(at, true), true)
	assert.Len(t, copyList, 1)
}

// timedObject is one object at a known time, for driving downsync end to end
// against a real local file.
type timedObject struct {
	system.ISystem
	at         time.Time
	noChecksum bool
	downloads  atomic.Int32
}

func (f *timedObject) Scheme() string                        { return "s3" }
func (f *timedObject) IsDirectory(_, _ string) (bool, error) { return true, nil }
func (f *timedObject) IsObject(_, _ string) (bool, error)    { return false, nil }

func (f *timedObject) List(bucket, prefix string, _ bool) ([]*system.FileObject, error) {
	fo := &system.FileObject{System: f, Bucket: bucket, Prefix: prefix + "/a.bin", Remote: true}
	fo.SetAttributes(&system.Attrs{Size: 5, ModTime: f.at,
		CalcCRC32C: func() (uint32, bool) { return 0, false },
		NoCRC32C:   func() bool { return f.noChecksum }})
	return []*system.FileObject{fo}, nil
}

func (f *timedObject) Download(_, _, _ string, _ bool, _ system.RunContext) error {
	f.downloads.Add(1)
	return nil
}

func downloadsFor(t *testing.T, objectAt, fileAt time.Time, noChecksum bool) int32 {
	t.Helper()
	dir := t.TempDir()
	file := filepath.Join(dir, "a.bin")
	assert.NoError(t, os.WriteFile(file, []byte("12345"), 0o644))
	assert.NoError(t, os.Chtimes(file, fileAt, fileAt))

	saved := pool
	defer func() { pool = saved }()
	pool = worker.New(1, false)
	pool.Run()
	sys := &timedObject{at: objectAt, noChecksum: noChecksum}
	src := &system.FileObject{System: sys, Bucket: "bucket", Prefix: "out", Remote: true}
	downsync(src, system.ParseFileObject(dir), true, true, true)
	pool.Close()
	return sys.downloads.Load()
}

func TestDownsyncSkipsAnUnchangedObjectWithNoChecksum(t *testing.T) {
	at := time.Unix(1700000000, 0)
	assert.Equal(t, int32(0), downloadsFor(t, at, at, true))
}

func TestDownsyncFetchesAReplacedObjectWithNoChecksum(t *testing.T) {
	at := time.Unix(1700000000, 0)
	assert.Equal(t, int32(1), downloadsFor(t, at.Add(time.Hour), at, true))
}

func TestDownsyncFetchesWhenTheLookupFailed(t *testing.T) {
	at := time.Unix(1700000000, 0)
	assert.Equal(t, int32(1), downloadsFor(t, at, at, false))
}
