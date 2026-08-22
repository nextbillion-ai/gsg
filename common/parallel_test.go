package common

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParallelDoRunsEveryIndexOnce(t *testing.T) {
	const n = 500
	counts := make([]int32, n)
	ParallelDo(n, 8, func(index int) { atomic.AddInt32(&counts[index], 1) })

	for i, c := range counts {
		assert.Equal(t, int32(1), c, "index %d", i)
	}
}

// The point of the change: work no longer starts one goroutine per item.
func TestParallelDoRespectsLimit(t *testing.T) {
	for _, limit := range []int{1, 4, 64} {
		var inFlight, peak int64
		ParallelDo(1000, limit, func(int) {
			cur := atomic.AddInt64(&inFlight, 1)
			for {
				old := atomic.LoadInt64(&peak)
				if cur <= old || atomic.CompareAndSwapInt64(&peak, old, cur) {
					break
				}
			}
			time.Sleep(time.Microsecond)
			atomic.AddInt64(&inFlight, -1)
		})
		assert.LessOrEqual(t, peak, int64(limit), "limit %d exceeded", limit)
		assert.Greater(t, peak, int64(0))
	}
}

// It must still be concurrent, not a disguised serial loop.
//
// Every wait here is bounded: a barrier inside the callbacks would deadlock
// the whole suite rather than fail this test if ParallelDo ran them serially.
func TestParallelDoActuallyRunsConcurrently(t *testing.T) {
	const limit = 8
	var inFlight int64
	allRunning := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce, allOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	// Unblocks the callbacks even when an assertion below ends the test early.
	defer releaseAll()

	done := make(chan struct{})
	go func() {
		defer close(done)
		ParallelDo(limit, limit, func(int) {
			if atomic.AddInt64(&inFlight, 1) == limit {
				allOnce.Do(func() { close(allRunning) })
			}
			<-release
		})
	}()

	select {
	case <-allRunning:
	case <-time.After(10 * time.Second):
		t.Fatal("fewer than the limit ever ran at once")
	}
	releaseAll()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("ParallelDo did not return")
	}
}

func TestParallelDoEdgeCases(t *testing.T) {
	var calls int64
	ParallelDo(0, 8, func(int) { atomic.AddInt64(&calls, 1) })
	assert.Equal(t, int64(0), calls, "no work means no calls")

	ParallelDo(-1, 8, func(int) { atomic.AddInt64(&calls, 1) })
	assert.Equal(t, int64(0), calls)

	// A non-positive limit degrades to serial rather than deadlocking.
	ParallelDo(5, 0, func(int) { atomic.AddInt64(&calls, 1) })
	assert.Equal(t, int64(5), calls)

	ParallelDo(3, 1000, func(int) { atomic.AddInt64(&calls, 1) })
	assert.Equal(t, int64(8), calls, "a limit above n is capped at n")
}
