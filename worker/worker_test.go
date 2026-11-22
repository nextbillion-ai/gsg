package worker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPool(t *testing.T) {
	res := new([3]int)
	pool := New(0, false)
	pool.Run()
	pool.Add(func() { res[0] = 1 })
	pool.Add(func() { res[1] = 2 })
	pool.Add(func() { res[2] = 3 })
	pool.Close()
	assert.Equal(t, 1, res[0])
	assert.Equal(t, 2, res[1])
	assert.Equal(t, 3, res[2])

	res = new([3]int)
	pool = New(1, false)
	pool.Run()
	pool.Add(func() { res[0] = 1 })
	pool.Add(func() { res[1] = 2 })
	pool.Add(func() { res[2] = 3 })
	pool.Close()
	assert.Equal(t, 1, res[0])
	assert.Equal(t, 2, res[1])
	assert.Equal(t, 3, res[2])

	res = new([3]int)
	pool = New(2, false)
	pool.Run()
	pool.Add(func() { res[0] = 1 })
	pool.Add(func() { res[1] = 2 })
	pool.Add(func() { res[2] = 3 })
	pool.Close()
	assert.Equal(t, 1, res[0])
	assert.Equal(t, 2, res[1])
	assert.Equal(t, 3, res[2])

	res = new([3]int)
	pool = New(3, false)
	pool.Run()
	pool.Add(func() { res[0] = 1 })
	pool.Add(func() { res[1] = 2 })
	pool.Add(func() { res[2] = 3 })
	pool.Close()
	assert.Equal(t, 1, res[0])
	assert.Equal(t, 2, res[1])
	assert.Equal(t, 3, res[2])

	res = new([3]int)
	pool = New(4, false)
	pool.Run()
	pool.Add(func() { res[0] = 1 })
	pool.Add(func() { res[1] = 2 })
	pool.Add(func() { res[2] = 3 })
	pool.Close()
	assert.Equal(t, 1, res[0])
	assert.Equal(t, 2, res[1])
	assert.Equal(t, 3, res[2])

	res = new([3]int)
	pool = New(3, false)
	pool.Run()
	pool.Close()
	assert.Equal(t, 0, res[0])
	assert.Equal(t, 0, res[1])
	assert.Equal(t, 0, res[2])

	res = new([3]int)
	pool = New(3, false)
	pool.Run()
	for i := 0; i < 3; i++ {
		j := i
		pool.Add(func() { res[j] = j + 1 })
	}
	pool.Close()
	assert.Equal(t, 1, res[0])
	assert.Equal(t, 2, res[1])
	assert.Equal(t, 3, res[2])
}

// A job may still be submitting to the next depth when Close is called, as an
// upload does with its parts once rsync has queued the last file.
func TestPoolCloseWaitsForJobsThatSubmitDeeper(t *testing.T) {
	for _, size := range []int{1, 2, 8} {
		pool := New(size, false)
		pool.Run()
		var done int32
		const files, parts = 5, 16
		for f := 0; f < files; f++ {
			pool.Add(func() {
				time.Sleep(20 * time.Millisecond)
				var wg sync.WaitGroup
				for i := 0; i < parts; i++ {
					wg.Add(1)
					pool.AddWithDepth(1, func() {
						defer wg.Done()
						atomic.AddInt32(&done, 1)
					})
				}
				wg.Wait()
			})
		}
		pool.Close()
		assert.Equal(t, int32(files*parts), atomic.LoadInt32(&done), "pool size %d", size)
	}
}

// A library caller's process is not gsg's to end: a job that panics is
// recovered and logged, and the worker goes on to the next one rather than
// leaving the pool a worker short.
func TestAPanickingJobDoesNotEndTheProcessOrTheWorker(t *testing.T) {
	pool := New(1, false)
	pool.Run()
	var ran atomic.Bool
	pool.Add(func() { panic("boom") })
	pool.Add(func() { ran.Store(true) })
	pool.Close()
	assert.True(t, ran.Load(), "the one worker ran the job after the panic")
}

// Without a pool the chunks still run in parallel, at most bound at once.
func TestRunChunksWithoutAPoolHonoursTheBound(t *testing.T) {
	for _, bound := range []int{1, 3, 16} {
		var inFlight, peak atomic.Int64
		var mu sync.Mutex
		seen := map[int]int{}
		RunChunks(nil, 64, bound, func(i int) {
			n := inFlight.Add(1)
			for {
				old := peak.Load()
				if n <= old || peak.CompareAndSwap(old, n) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			mu.Lock()
			seen[i]++
			mu.Unlock()
			inFlight.Add(-1)
		})
		assert.Len(t, seen, 64, "bound %d: every chunk ran", bound)
		assert.LessOrEqual(t, peak.Load(), int64(bound), "bound %d", bound)
		if bound > 1 {
			assert.Greater(t, peak.Load(), int64(1), "bound %d: in parallel", bound)
		}
	}
}
