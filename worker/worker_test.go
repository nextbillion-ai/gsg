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
