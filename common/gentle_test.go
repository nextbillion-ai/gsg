package common

import (
	"bytes"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

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
