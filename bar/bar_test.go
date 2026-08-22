package bar

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The printer goroutine ranged over Container.bars with no lock while New
// appended under one. Run with -race.
func TestContainerNewIsRaceFreeWithPrinter(t *testing.T) {
	c, err := New()
	assert.NoError(t, err)

	// Append in rounds spread over more than one printer tick (50ms), with a
	// barrier per round, so the appends genuinely overlap the printer's read
	// rather than racing to finish before its first pass.
	const rounds, perRound = 4, 50
	for r := 0; r < rounds; r++ {
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < perRound; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				c.New(int64(1000+i), "bar")
			}(i)
		}
		close(start)
		wg.Wait()
		time.Sleep(20 * time.Millisecond)
	}

	c.Lock()
	assert.Equal(t, rounds*perRound, len(c.bars))
	c.Unlock()
}

// Every chunk goroutine of one download writes to the same bar while the
// printer reads it. Run with -race.
func TestProgressBarConcurrentIncrBy(t *testing.T) {
	c, err := New()
	assert.NoError(t, err)

	const writers, perWriter = 16, 500
	p := c.New(writers*perWriter, "chunked")

	// A barrier so the writers and the reader are all in flight together.
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < perWriter; j++ {
				_, _ = p.Write([]byte{0})
			}
		}()
	}
	// Read concurrently, as the printer does.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 2000; i++ {
			p.drawSimple()
		}
	}()
	close(start)
	wg.Wait()

	// No lost updates: every byte written is accounted for.
	p.mu.Lock()
	defer p.mu.Unlock()
	assert.Equal(t, int64(writers*perWriter), p.Progress)
}

func TestProgressBarIncrByClampsToTotal(t *testing.T) {
	c, _ := New()
	p := c.New(10, "small")
	p.IncrBy(4)
	p.IncrBy(100)

	p.mu.Lock()
	defer p.mu.Unlock()
	assert.Equal(t, int64(10), p.Progress)
}

// humanizeBytes indexed a 7-element slice with an unclamped exponent, so a
// large or non-finite speed panicked with "index out of range [7] with length 7".
func TestHumanizeBytesNeverPanics(t *testing.T) {
	for _, s := range []float64{
		0, 1, 9, 10, 1024, 1e6, 1e18,
		math.Pow(1024, 7), 1.2e21, 1e25, math.MaxFloat64,
		math.Inf(1), math.Inf(-1), math.NaN(), -1, -1e30,
	} {
		assert.NotPanics(t, func() { humanizeBytes(s) }, "humanizeBytes(%g)", s)
	}
}

func TestHumanizeBytesFormats(t *testing.T) {
	assert.Equal(t, " 0B", humanizeBytes(0))
	assert.Equal(t, " 9B", humanizeBytes(9))
	assert.Equal(t, "1.0kB", humanizeBytes(1024))
	assert.Equal(t, "1.0MB", humanizeBytes(1024*1024))
	// Anything past the last unit is reported in it rather than overflowing.
	assert.Contains(t, humanizeBytes(math.Pow(1024, 7)), "EB")
	assert.Contains(t, humanizeBytes(math.Inf(1)), "EB")
}
