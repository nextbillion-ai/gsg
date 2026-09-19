package oci

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nextbillion-ai/gsg/bar"
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/worker"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The geometry has to cover the object exactly: every byte in one chunk, no
// byte in two, and nothing past the end. A download assembles the file from
// these offsets, so an error here is a corrupt file rather than a failure.
func TestDownloadGeometryCoversTheObjectExactly(t *testing.T) {
	for _, size := range []int64{0, 1, 4095, 1 << 20, 16 * 1024 * 1024, 16*1024*1024 + 1, 1 << 30} {
		for _, requested := range []int64{-1, 0, 1, 4096, 1 << 20, 1 << 30} {
			chunkSize, chunks := downloadGeometry(size, requested)
			where := fmt.Sprintf("size %d requested %d", size, requested)
			require.Greater(t, chunkSize, int64(0), where)
			require.GreaterOrEqual(t, chunks, 1, where)

			// The last chunk must be the only short one, and there must be no
			// chunk past the end of the object.
			require.Less(t, int64(chunks-1)*chunkSize, max64(size, 1), where)
			require.GreaterOrEqual(t, int64(chunks)*chunkSize, size, where)

			if chunks > 4096 {
				continue // the invariants above already say it adds up
			}
			var covered int64
			for i := 0; i < chunks; i++ {
				off := int64(i) * chunkSize
				length := chunkSize
				if off+length > size {
					length = size - off
				}
				require.GreaterOrEqual(t, length, int64(0), "%s chunk %d", where, i)
				require.Equal(t, covered, off, "%s: chunk %d starts where the last ended", where, i)
				covered += length
			}
			assert.Equal(t, size, covered, "%s: the chunks cover the object", where)
		}
	}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// --chunk-size is read the way the other two backends read it.
func TestDownloadGeometryHonoursTheFlag(t *testing.T) {
	chunkSize, chunks := downloadGeometry(100*1024*1024, -1)
	assert.Equal(t, defaultDownloadChunkSize, chunkSize, "no preference means the default")
	assert.Equal(t, 7, chunks)

	// zero means "do not chunk": one request for the whole object
	_, chunks = downloadGeometry(100*1024*1024, 0)
	assert.Equal(t, 1, chunks)

	chunkSize, chunks = downloadGeometry(10, 4)
	assert.Equal(t, int64(4), chunkSize)
	assert.Equal(t, 3, chunks)

	// an empty object is one chunk of nothing, whatever was asked for
	for _, requested := range []int64{-1, 0, 1 << 20} {
		_, chunks = downloadGeometry(0, requested)
		assert.Equal(t, 1, chunks, "requested %d", requested)
	}
}

// RFC 7233 endpoints are inclusive. Getting this wrong is not loud -- the
// service clamps the overshoot and the neighbouring chunk rewrites the shared
// byte with the same value -- so only the arithmetic says so.
func TestRangeHeaderEndpointsAreInclusive(t *testing.T) {
	h, ranged := rangeHeader(0, 100)
	assert.True(t, ranged)
	assert.Equal(t, "bytes=0-99", h)

	h, _ = rangeHeader(100, 100)
	assert.Equal(t, "bytes=100-199", h, "chunks must not overlap by a byte")

	h, _ = rangeHeader(1<<30, 1)
	assert.Equal(t, fmt.Sprintf("bytes=%d-%d", 1<<30, 1<<30), h, "a single byte")

	// an empty object: no range at all, rather than "bytes=0--1"
	h, ranged = rangeHeader(0, 0)
	assert.False(t, ranged)
	assert.Empty(t, h)

	// negative length is "to the end"
	h, ranged = rangeHeader(0, -1)
	assert.False(t, ranged, "from the start to the end is the whole object")
	h, ranged = rangeHeader(4096, -1)
	assert.True(t, ranged)
	assert.Equal(t, "bytes=4096-", h)

	h, ranged = rangeHeader(-5, 10)
	assert.True(t, ranged)
	assert.Equal(t, "bytes=0-9", h, "a negative offset cannot produce a negative range")
}

// Every chunk has to run exactly once, whether or not there is a pool, and
// concurrency has to be bounded either way.
func TestRunChunksRunsEveryChunkOnceAndBoundsThem(t *testing.T) {
	const chunks = 64
	for _, withPool := range []bool{false, true} {
		var pool *worker.Pool
		if withPool {
			pool = worker.New(4, false)
			pool.Run()
		}

		var mu sync.Mutex
		seen := map[int]int{}
		var inFlight, peak int64
		runChunks(pool, chunks, func(i int) {
			n := atomic.AddInt64(&inFlight, 1)
			for {
				old := atomic.LoadInt64(&peak)
				if n <= old || atomic.CompareAndSwapInt64(&peak, old, n) {
					break
				}
			}
			mu.Lock()
			seen[i]++
			mu.Unlock()
			atomic.AddInt64(&inFlight, -1)
		})
		if pool != nil {
			pool.Close()
		}

		assert.Len(t, seen, chunks, "with pool %v: every chunk ran", withPool)
		for i := 0; i < chunks; i++ {
			assert.Equal(t, 1, seen[i], "with pool %v: chunk %d ran once", withPool, i)
		}
		assert.LessOrEqual(t, peak, int64(chunks), "with pool %v: concurrency is bounded", withPool)
	}
}

// The nil pool is the library caller's case -- jam-core passes a RunContext
// with a ChunkSize and nothing else. It must not panic, and it must not
// silently become a serial download either.
func TestRunChunksWithoutAPoolStillRunsInParallel(t *testing.T) {
	const chunks = 8
	// The first two chunks each wait for the other to have started, so a
	// runner that runs them one after another never finishes.
	started := make(chan struct{}, 2)
	proceed := make(chan struct{})
	go func() {
		<-started
		<-started
		close(proceed)
	}()

	var done int64
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		runChunks(nil, chunks, func(i int) {
			if i < 2 {
				started <- struct{}{}
				<-proceed
			}
			atomic.AddInt64(&done, 1)
		})
	}()

	select {
	case <-finished:
		assert.Equal(t, int64(chunks), atomic.LoadInt64(&done))
	case <-time.After(10 * time.Second):
		t.Fatal("chunks ran one at a time: a caller that passes no pool gets a serial download")
	}
}

// Bounding the fetches is not the same as bounding the goroutines. A slot
// taken inside the goroutine still creates one per chunk up front, and the
// chunk count follows the object: an 80 GiB download at a 1 MiB --chunk-size
// is 81920 of them, parked, before the first byte moves.
func TestRunChunksWithoutAPoolDoesNotStartAGoroutinePerChunk(t *testing.T) {
	const chunks = 5000
	base := runtime.NumGoroutine()
	var peak int64
	runChunks(nil, chunks, func(int) {
		grew := int64(runtime.NumGoroutine() - base)
		for {
			old := atomic.LoadInt64(&peak)
			if grew <= old || atomic.CompareAndSwapInt64(&peak, old, grew) {
				break
			}
		}
		time.Sleep(time.Microsecond)
	})
	assert.Less(t, peak, int64(100), "one goroutine per chunk is waiting to run: %d of them", peak)
}

// GetObjectRangeReader asked for nothing must give nothing. An absent range
// header means "the whole object" -- which is what the unranged reader relies
// on it meaning -- so a zero length that reached the request would hand the
// caller the entire object instead.
func TestGetObjectRangeReaderOfNothingReadsNothing(t *testing.T) {
	rc, err := (&OCI{}).GetObjectRangeReader("bucket", "key", 4096, 0)
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestSettleDownloadSaysWhatItCanAndFailsOnlyWhenAsked(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "obj")
	content := []byte("some bytes")
	require.NoError(t, os.WriteFile(path, content, 0o644))
	crc := crc32.Checksum(content, common.Castagnoli)

	// nothing measured, because nothing was asked for
	assert.NoError(t, settleDownload(false, path, "b", "o", 0, false, crc, true))

	// the object carries no checksum: not comparable, and not a failure
	assert.NoError(t, settleDownload(true, path, "b", "o", crc, true, 0, false))

	// agreement
	assert.NoError(t, settleDownload(true, path, "b", "o", crc, true, crc, true))
	assert.Equal(t, crc, common.GetFileCRC32C(path), "a verified download is cached")

	// disagreement: reported either way, fatal only under -v
	assert.Error(t, settleDownload(true, path, "b", "o", crc^1, true, crc, true))
	assert.NoError(t, settleDownload(false, path, "b", "o", crc^1, true, crc, true),
		"without -v a mismatch is reported, not fatal")
}

// A retried chunk reports its bytes twice unless the failed attempt is wound
// back, and the bar then runs ahead of the transfer.
func TestFetchWithRetryWindsTheBarBackOnAFailedAttempt(t *testing.T) {
	pb := &bar.ProgressBar{Total: 1 << 20}
	attempts := 0
	sum, n, err := fetchWithRetry(pb, func() (uint32, int64, error) {
		attempts++
		if attempts == 1 {
			pb.IncrBy(4096) // what this attempt managed to write
			return 0, 4096, fmt.Errorf("connection reset by peer")
		}
		pb.IncrBy(8192)
		return 0xfeedface, 8192, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, attempts, "the chunk was retried, not the transfer")
	assert.Equal(t, uint32(0xfeedface), sum, "the sum is the successful attempt's")
	assert.Equal(t, int64(8192), n)
	assert.Equal(t, int64(8192), pb.Progress, "the failed attempt's bytes were counted as well")
}

func TestFetchWithRetryGivesUpAndLeavesTheBarWhereItWas(t *testing.T) {
	pb := &bar.ProgressBar{Total: 1 << 20}
	attempts := 0
	_, _, err := fetchWithRetry(pb, func() (uint32, int64, error) {
		attempts++
		pb.IncrBy(1024)
		return 0, 1024, fmt.Errorf("no such host")
	})
	assert.Error(t, err)
	assert.Greater(t, attempts, 1, "a failing chunk is tried more than once")
	assert.Equal(t, int64(0), pb.Progress, "every abandoned attempt was wound back")
}

// A nil *bar.ProgressBar passed as an io.Writer is not a nil io.Writer: the
// interface carries the type, so the callee writes to it and IncrBy
// dereferences the nil receiver. A caller with no bars is ordinary -- the
// library API builds a RunContext without them -- so this is the common path.
func TestAGentleChunkWithNoProgressBarDoesNotPanic(t *testing.T) {
	content := []byte("sixteen bytes!!!")
	path := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.WriteFile(path, make([]byte, len(content)), 0o644))
	fl, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer func() { _ = fl.Close() }()
	verifier, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = verifier.Close() }()

	var absent *bar.ProgressBar
	assert.Nil(t, progressWriter(absent), "no bar means nothing to write to")

	_, n, werr := common.GentleWrite(fl, verifier, 0, bytes.NewReader(content), progressWriter(absent))
	require.NoError(t, werr)
	assert.Equal(t, int64(len(content)), n)

	// And the hazard the helper exists to prevent, pinned rather than
	// described: handing the same nil bar over directly is a write to a
	// non-nil interface holding a nil pointer.
	again, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer func() { _ = again.Close() }()
	assert.Panics(t, func() {
		_, _, _ = common.GentleWrite(again, verifier, 0, bytes.NewReader(content), absent)
	}, "if this stops panicking, bar.IncrBy nil-checks and progressWriter is no longer load-bearing")
}

// Verifying an object that carries no checksum means hashing the whole file to
// compare it against nothing, and then printing "skipped". On an 80 GiB object
// written by another tool that is a full pass over the disk for no answer.
func TestAVerifiedDownloadOnlyReadsBackWhenThereIsSomethingToCompare(t *testing.T) {
	assert.True(t, readBackToVerify(true, true), "-v against an object that has a checksum")
	assert.False(t, readBackToVerify(true, false), "-v against an object that has none")
	assert.False(t, readBackToVerify(false, true), "no -v was asked for")
	assert.False(t, readBackToVerify(false, false))
}
