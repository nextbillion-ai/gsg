package common

import (
	"bytes"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdviseRangeCoversEveryWindowTwiceAndNoMore(t *testing.T) {
	const chunk = 95*1024*1024 + 123
	asked := make([]int, chunk/(1<<20)+1)
	var start, previous, total int64
	for start < chunk {
		length := int64(GentleWindow)
		if start+length > chunk {
			length = chunk - start
		}
		offset, n := adviseRange(start, length, previous)
		require.True(t, n > 0 && offset >= 0 && offset+n == start+length, "window at %d: [%d,+%d)", start, offset, n)
		for mb := offset >> 20; mb <= (offset+n-1)>>20; mb++ {
			asked[mb]++
		}
		total += n
		start, previous = start+length, length
	}
	assert.LessOrEqual(t, total, int64(2*chunk), "requests stay linear in the chunk")
	for mb, times := range asked[:len(asked)-1] {
		assert.GreaterOrEqual(t, times, 1, "MB %d is never asked for", mb)
		assert.LessOrEqual(t, times, 3, "MB %d is asked for again and again", mb)
	}
}

// gentleWrite is only worth anything if the file it leaves behind is the file
// that was sent and the sum it returns describes it. Sizes either side of a
// window boundary, since the last window is the partial one.
func TestGentleWriteLandsTheBytesAndSumsWhatLanded(t *testing.T) {
	for _, size := range []int{0, 1, 1024, GentleWindow - 1, GentleWindow, GentleWindow + 7, 2*GentleWindow + 3} {
		content := make([]byte, size)
		for i := range content {
			content[i] = byte(i * 7)
		}
		path := filepath.Join(t.TempDir(), "out")
		require.NoError(t, os.WriteFile(path, make([]byte, size), 0o644))

		fl, err := os.OpenFile(path, os.O_WRONLY, 0o644)
		require.NoError(t, err)
		verifier, err := os.Open(path)
		require.NoError(t, err)

		var progress bytes.Buffer
		crc, n, werr := GentleWrite(fl, verifier, 0, bytes.NewReader(content), &progress)
		require.NoError(t, werr)
		require.NoError(t, fl.Close())
		require.NoError(t, verifier.Close())

		assert.Equal(t, int64(size), n, "size %d: byte count", size)
		assert.Equal(t, crc32.Checksum(content, Castagnoli), crc, "size %d: checksum", size)
		assert.Equal(t, int64(size), int64(progress.Len()), "size %d: progress", size)

		landed, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, content, landed, "size %d: contents", size)
	}
}

// Chunks are written at their own offsets, in whatever order the pool runs
// them, and each sums only its own bytes -- fold them and the whole file comes
// back. This is the property the download's verification rests on.
func TestGentleWriteSumsOnlyItsOwnChunk(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789abcdef"), 1<<14) // 256 KiB
	const chunkSize = 100 * 1024

	path := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.WriteFile(path, make([]byte, len(content)), 0o644))

	var sums []uint32
	var lens []int64
	// backwards, so that a chunk cannot be relying on the one before it
	type chunk struct{ off, length int64 }
	var chunks []chunk
	for off := int64(0); off < int64(len(content)); off += chunkSize {
		length := int64(chunkSize)
		if off+length > int64(len(content)) {
			length = int64(len(content)) - off
		}
		chunks = append(chunks, chunk{off, length})
		sums = append(sums, 0)
		lens = append(lens, 0)
	}
	for i := len(chunks) - 1; i >= 0; i-- {
		c := chunks[i]
		fl, err := os.OpenFile(path, os.O_WRONLY, 0o644)
		require.NoError(t, err)
		_, err = fl.Seek(c.off, io.SeekStart)
		require.NoError(t, err)
		verifier, err := os.Open(path)
		require.NoError(t, err)
		crc, n, werr := GentleWrite(fl, verifier, c.off, bytes.NewReader(content[c.off:c.off+c.length]), nil)
		require.NoError(t, werr)
		require.NoError(t, fl.Close())
		require.NoError(t, verifier.Close())
		sums[i], lens[i] = crc, n
	}

	crc, total := FoldCRC32C(sums, lens)
	assert.Equal(t, int64(len(content)), total)
	assert.Equal(t, crc32.Checksum(content, Castagnoli), crc)

	landed, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, content, landed)
}

// A read that fails partway has to say how much it wrote, because the caller
// retrying the chunk uses that to undo what the failed attempt reported.
func TestGentleWriteReportsWhatItWroteBeforeFailing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.WriteFile(path, make([]byte, 1<<20), 0o644))
	fl, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer func() { _ = fl.Close() }()
	verifier, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = verifier.Close() }()

	src := io.MultiReader(bytes.NewReader(make([]byte, 4096)), failingReader{})
	_, n, werr := GentleWrite(fl, verifier, 0, src, nil)
	assert.Error(t, werr)
	assert.Equal(t, int64(4096), n)
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, assert.AnError }

// dropRequest is one range the transfer asked the kernel to drop.
type dropRequest struct{ offset, length int64 }

// withRecordedPacing swaps the sleep and the advice for recorders, since
// neither is observable otherwise: the advice is a no-op on every platform but
// linux, and a sleep leaves no trace. A gentle mode that paces nothing at all
// is the defect these pin, and it has now been shipped twice.
func withRecordedPacing(t *testing.T) (*[]time.Duration, *[]dropRequest) {
	t.Helper()
	var slept []time.Duration
	var dropped []dropRequest
	sleep, advise := gentleSleep, gentleAdviseDrop
	gentleSleep = func(d time.Duration) { slept = append(slept, d) }
	gentleAdviseDrop = func(_ *os.File, offset, length int64) {
		dropped = append(dropped, dropRequest{offset, length})
	}
	t.Cleanup(func() { gentleSleep, gentleAdviseDrop = sleep, advise })
	return &slept, &dropped
}

func gentleWriteOf(t *testing.T, size int, offset int64) (uint32, int64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.WriteFile(path, make([]byte, offset+int64(size)), 0o644))
	fl, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer func() { _ = fl.Close() }()
	_, err = fl.Seek(offset, io.SeekStart)
	require.NoError(t, err)
	verifier, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = verifier.Close() }()

	crc, n, werr := GentleWrite(fl, verifier, offset, bytes.NewReader(bytes.Repeat([]byte("z"), size)), nil)
	require.NoError(t, werr)
	return crc, n
}

// A call shorter than a window never completes one, so the loop's sleep never
// fires and its single drop request only starts writeback. Left there, gentle
// mode at a 1 MiB --chunk-size paces nothing and evicts nothing -- present,
// and doing nothing.
func TestGentleWriteShorterThanAWindowIsStillPaced(t *testing.T) {
	slept, dropped := withRecordedPacing(t)
	const size = 1 << 20
	_, n := gentleWriteOf(t, size, 4096)
	require.Equal(t, int64(size), n)

	require.NotEmpty(t, *slept, "a chunk smaller than a window was not paced at all")
	var total time.Duration
	for _, d := range *slept {
		total += d
	}
	assert.Equal(t, gentlePause*size/GentleWindow, total, "the rate must not depend on the chunk size")

	// The window it wrote has to be asked for twice: once as the window, and
	// once more by the request that closes the call, or the pages only ever
	// start their writeback.
	covered := 0
	for _, d := range *dropped {
		if d.offset <= 4096 && d.offset+d.length >= 4096+size {
			covered++
		}
	}
	assert.GreaterOrEqual(t, covered, 2, "the tail is asked for once and so is never dropped: %v", *dropped)
}

// The rate is the same however the caller cuts the transfer up: this is what
// makes --chunk-size a size rather than a pacing knob.
func TestGentleWritePacesAtTheSameRateWhateverTheChunkSize(t *testing.T) {
	for _, size := range []int{1 << 20, GentleWindow, GentleWindow + 1, 3 * GentleWindow} {
		slept, _ := withRecordedPacing(t)
		_, n := gentleWriteOf(t, size, 0)
		require.Equal(t, int64(size), n)

		var total time.Duration
		for _, d := range *slept {
			total += d
		}
		assert.Equal(t, gentlePause*time.Duration(size)/GentleWindow, total, "size %d", size)
	}
}

// Every byte written has to be asked for at least twice, or some of it stays
// in the page cache -- which is the whole point of the mode.
func TestGentleWriteAsksForEveryByteTwice(t *testing.T) {
	const size = 3*GentleWindow + 1234
	const offset = 1 << 16
	_, dropped := withRecordedPacing(t)
	_, n := gentleWriteOf(t, size, offset)
	require.Equal(t, int64(size), n)

	asked := make([]int, size/(1<<20)+1)
	for _, d := range *dropped {
		require.GreaterOrEqual(t, d.offset, int64(offset), "a request reached outside the chunk: %+v", d)
		require.LessOrEqual(t, d.offset+d.length, int64(offset+size), "a request reached past the chunk: %+v", d)
		for mb := (d.offset - offset) >> 20; mb <= (d.offset-offset+d.length-1)>>20; mb++ {
			asked[mb]++
		}
	}
	// Including the last, which is the one with no window after it to ask
	// again on its behalf -- so it is the byte range that stays cached when
	// the call does not close itself out.
	for mb, times := range asked {
		assert.GreaterOrEqual(t, times, 2, "MB %d is asked for %d time(s), so it only starts writeback", mb, times)
	}
}
