package common

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tlsRecords hands out at most one TLS record's worth per Read, as a response
// body over HTTP/1.1 does.
type tlsRecords struct{ r io.Reader }

func (t tlsRecords) Read(p []byte) (int, error) {
	if len(p) > 16<<10 {
		p = p[:16<<10]
	}
	return t.r.Read(p)
}

// writeSizes records the size of each write it is given.
type writeSizes struct{ sizes []int }

func (w *writeSizes) Write(p []byte) (int, error) {
	w.sizes = append(w.sizes, len(p))
	return len(p), nil
}

// Each write is a full buffer however little each Read returns: written one
// record at a time from many chunks into one file, a gentle download spent most
// of its CPU in the kernel. The progress writer is handed exactly what is
// written to the file, so its calls are the file's writes.
func TestGentleWriteFillsItsBufferBeforeWriting(t *testing.T) {
	const size = 2*gentleBuffer + 12345
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i * 13)
	}
	path := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.WriteFile(path, make([]byte, size), 0o644))
	fl, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer func() { _ = fl.Close() }()

	writes := &writeSizes{}
	crc, n, werr := GentleWrite(fl, 0, 0, tlsRecords{bytes.NewReader(content)}, writes)
	require.NoError(t, werr)
	assert.Equal(t, int64(size), n)
	assert.Equal(t, crc32.Checksum(content, Castagnoli), crc)
	assert.Equal(t, []int{gentleBuffer, gentleBuffer, 12345}, writes.sizes)

	landed, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, content, landed)
}

// A chunk smaller than the buffer holds only its own size, so many small
// chunks at once do not each hold gentleBuffer.
func TestGentleWriteSizesItsBufferToTheChunk(t *testing.T) {
	const size = 3<<20 + 5
	path := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.WriteFile(path, make([]byte, size), 0o644))
	fl, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer func() { _ = fl.Close() }()

	writes := &writeSizes{}
	_, n, werr := GentleWrite(fl, 0, 1<<20, tlsRecords{bytes.NewReader(make([]byte, size))}, writes)
	require.NoError(t, werr)
	assert.Equal(t, int64(size), n)
	assert.Equal(t, []int{1 << 20, 1 << 20, 1 << 20, 5}, writes.sizes)
}

// A source that fails partway still has what it read written and counted, so
// a retried chunk can wind its progress back by the right amount.
func TestGentleWriteWritesWhatItFilledBeforeAFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.WriteFile(path, make([]byte, 1<<20), 0o644))
	fl, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer func() { _ = fl.Close() }()

	src := io.MultiReader(tlsRecords{bytes.NewReader(bytes.Repeat([]byte("q"), 40000))}, failingReader{})
	writes := &writeSizes{}
	_, n, werr := GentleWrite(fl, 0, 0, src, writes)
	assert.ErrorIs(t, werr, assert.AnError)
	assert.Equal(t, int64(40000), n)
	assert.Equal(t, []int{40000}, writes.sizes)
}

// BenchmarkGentleWriteParallelChunks is the shape of a chunked gentle download:
// 16 chunks written at their own offsets into one file at once, each from a
// source that returns one TLS record per Read. Pacing is off, so what is
// measured is the writing.
//
//	go test ./common -run '^$' -bench GentleWriteParallelChunks -benchtime 5x
func BenchmarkGentleWriteParallelChunks(b *testing.B) {
	const chunks, chunkSize = 16, 64 << 20
	sleep := gentleSleep
	gentleSleep = func(d time.Duration) {}
	defer func() { gentleSleep = sleep }()
	record := bytes.Repeat([]byte("0123456789abcdef"), 1<<10)
	path := filepath.Join(b.TempDir(), "out")
	b.SetBytes(chunks * chunkSize)
	for i := 0; i < b.N; i++ {
		require.NoError(b, CreateFile(path, chunks*chunkSize))
		var wg sync.WaitGroup
		errs := make([]error, chunks)
		for c := 0; c < chunks; c++ {
			wg.Add(1)
			go func(c int) {
				defer wg.Done()
				off := int64(c) * chunkSize
				fl, err := os.OpenFile(path, os.O_WRONLY, 0o644)
				if err != nil {
					errs[c] = err
					return
				}
				defer func() { _ = fl.Close() }()
				if _, err = fl.Seek(off, io.SeekStart); err != nil {
					errs[c] = err
					return
				}
				src := tlsRecords{io.LimitReader(repeatReader(record), chunkSize)}
				_, n, err := GentleWrite(fl, off, 0, src, nil)
				if err == nil && n != chunkSize {
					err = fmt.Errorf("chunk %d wrote %d", c, n)
				}
				errs[c] = err
			}(c)
		}
		wg.Wait()
		for _, err := range errs {
			require.NoError(b, err)
		}
	}
}

// repeatReader reads p over and over, without end.
func repeatReader(p []byte) io.Reader {
	return &repeating{p: p}
}

type repeating struct {
	p   []byte
	off int
}

func (r *repeating) Read(b []byte) (int, error) {
	n := 0
	for n < len(b) {
		c := copy(b[n:], r.p[r.off:])
		n += c
		r.off = (r.off + c) % len(r.p)
	}
	return n, nil
}
