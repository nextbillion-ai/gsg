package worker

import (
	"testing"

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
