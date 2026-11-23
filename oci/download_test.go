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
		runChunks(pool, chunks, 0, func(i int) {
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
		runChunks(nil, chunks, 0, func(i int) {
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
	runChunks(nil, chunks, 0, func(int) {
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
	content := []byte("some bytes")
	crc := crc32.Checksum(content, common.Castagnoli)
	size := int64(len(content))
	const path = "/nonexistent/obj" // settled before the rename: no file is read

	// the object carries no checksum: not comparable, not a failure, put in
	// place but not cached
	for _, verify := range []bool{true, false} {
		keep, cache, err := settleDownload(verify, path, "b", "o", crc^1, size, size, 0, false)
		assert.NoError(t, err)
		assert.True(t, keep, "-v %t: nothing to compare with is not a mismatch", verify)
		assert.False(t, cache, "-v %t: nothing is cached for an object without a checksum", verify)
	}

	// agreement, with and without -v: in place and cached either way
	for _, verify := range []bool{true, false} {
		keep, cache, err := settleDownload(verify, path, "b", "o", crc, size, size, crc, true)
		assert.NoError(t, err)
		assert.True(t, keep, "-v %t", verify)
		assert.True(t, cache, "-v %t: the sum taken while writing is cached", verify)
	}

	// disagreement: reported either way, fatal only under -v, never put in place
	for _, verify := range []bool{true, false} {
		keep, cache, err := settleDownload(verify, path, "b", "o", crc^1, size, size, crc, true)
		if verify {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err, "without -v a mismatch is reported, not fatal")
		}
		assert.False(t, keep, "-v %t: a mismatching file does not replace the destination", verify)
		assert.False(t, cache, "-v %t: a mismatching sum is not cached", verify)
	}

	// a sum over fewer bytes than the object has is a mismatch, whatever it is
	keep, _, err := settleDownload(true, path, "b", "o", crc, size-1, size, crc, true)
	assert.Error(t, err)
	assert.False(t, keep)
}

// cachedCRC32C returns what GetFileCRC32C answers for path once its content no
// longer adds up to it: only a cached value survives that.
func cachedCRC32C(t *testing.T, path string) uint32 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte("x"), int(info.Size())), 0o644))
	require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime()))
	return common.GetFileCRC32C(path)
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

	var absent *bar.ProgressBar
	assert.Nil(t, progressWriter(absent), "no bar means nothing to write to")

	_, n, werr := common.GentleWrite(fl, 0, 0, bytes.NewReader(content), progressWriter(absent))
	require.NoError(t, werr)
	assert.Equal(t, int64(len(content)), n)

	// The same nil bar handed over directly is a non-nil interface holding a
	// nil pointer; the bar accepts that too, since gs and s3 pass theirs on
	// without the helper.
	again, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer func() { _ = again.Close() }()
	assert.NotPanics(t, func() {
		_, _, _ = common.GentleWrite(again, 0, 0, bytes.NewReader(content), absent)
	})
}
