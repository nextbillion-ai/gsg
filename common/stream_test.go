package common

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var fastRetry = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(int64(n))).Read(b)
	return b
}

// fakeObject serves ranges of data. hook, when set, may replace the reader
// for one open or fail it.
type fakeObject struct {
	data []byte
	mu   sync.Mutex
	// opens records each (off, n) asked for
	opens    [][2]int64
	inFlight atomic.Int32
	maxIn    atomic.Int32
	hook     func(call int, off, n int64, r io.Reader) (io.Reader, error)
}

type countedReader struct {
	io.Reader
	o    *fakeObject
	once sync.Once
}

func (c *countedReader) Close() error {
	c.once.Do(func() { c.o.inFlight.Add(-1) })
	return nil
}

func (o *fakeObject) source() StreamSource {
	return StreamSource{
		Name:      "fake://obj",
		Size:      int64(len(o.data)),
		CRC32C:    crc32.Checksum(o.data, Castagnoli),
		HasCRC32C: true,
		Open: func(ctx context.Context, off, n int64) (io.ReadCloser, error) {
			o.mu.Lock()
			call := len(o.opens)
			o.opens = append(o.opens, [2]int64{off, n})
			o.mu.Unlock()
			end := int64(len(o.data))
			if n >= 0 && off+n < end {
				end = off + n
			}
			var r io.Reader = bytes.NewReader(o.data[off:end])
			if o.hook != nil {
				var err error
				if r, err = o.hook(call, off, n, r); err != nil {
					return nil, err
				}
			}
			cur := o.inFlight.Add(1)
			for {
				m := o.maxIn.Load()
				if cur <= m || o.maxIn.CompareAndSwap(m, cur) {
					break
				}
			}
			return &countedReader{Reader: r, o: o}, nil
		},
	}
}

func TestStreamWritesExactBytesInOrder(t *testing.T) {
	cases := []struct {
		size, chunk int64
		window      int
	}{
		{0, 16, 4},
		{1, 16, 4},
		{100, 16, 4},
		{128, 16, 4},
		{129, 16, 1},
		{1000, 7, 3},
		{1000, 0, 4},
		{1000, 5000, 4},
		{1 << 20, 4096, 8},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("size=%d chunk=%d window=%d", tc.size, tc.chunk, tc.window), func(t *testing.T) {
			o := &fakeObject{data: randomBytes(int(tc.size))}
			var out bytes.Buffer
			err := Stream(context.Background(), &out, o.source(), StreamOptions{ChunkSize: tc.chunk, Window: tc.window, Retry: fastRetry})
			require.NoError(t, err)
			assert.True(t, bytes.Equal(o.data, out.Bytes()))
			assert.LessOrEqual(t, int(o.maxIn.Load()), tc.window)
		})
	}
}

// The first chunk is written while it is still arriving, not once it is whole.
func TestStreamWritesBeforeTheFirstChunkCompletes(t *testing.T) {
	o := &fakeObject{data: randomBytes(1 << 16)}
	gate := make(chan struct{})
	o.hook = func(call int, off, n int64, r io.Reader) (io.Reader, error) {
		if off != 0 {
			return r, nil
		}
		// 100 bytes, then wait for the test to see them written
		return io.MultiReader(io.LimitReader(r, 100), readerFunc(func(p []byte) (int, error) {
			<-gate
			return r.Read(p)
		})), nil
	}
	w := &signalWriter{first: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- Stream(context.Background(), w, o.source(), StreamOptions{ChunkSize: 1 << 14, Window: 2, Retry: fastRetry})
	}()
	select {
	case <-w.first:
	case <-time.After(5 * time.Second):
		t.Fatal("nothing was written while the first chunk was incomplete")
	}
	close(gate)
	require.NoError(t, <-done)
	assert.True(t, bytes.Equal(o.data, w.buf.Bytes()))
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

type signalWriter struct {
	buf   bytes.Buffer
	first chan struct{}
	once  sync.Once
}

func (w *signalWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.first) })
	return w.buf.Write(p)
}

// A connection reset mid-chunk reopens the range at the byte it reached.
func TestStreamResumesAChunkWhereItBroke(t *testing.T) {
	for _, chunk := range []int64{1000, 0} {
		t.Run(fmt.Sprintf("chunk=%d", chunk), func(t *testing.T) {
			o := &fakeObject{data: randomBytes(10000)}
			var resets atomic.Int32
			o.hook = func(call int, off, n int64, r io.Reader) (io.Reader, error) {
				if off%1000 == 0 && resets.Load() < 20 {
					resets.Add(1)
					return io.MultiReader(io.LimitReader(r, 300), errReader{syscall.ECONNRESET}), nil
				}
				return r, nil
			}
			var out bytes.Buffer
			err := Stream(context.Background(), &out, o.source(), StreamOptions{ChunkSize: chunk, Window: 4, Retry: fastRetry})
			require.NoError(t, err)
			assert.True(t, bytes.Equal(o.data, out.Bytes()))
			o.mu.Lock()
			defer o.mu.Unlock()
			for _, op := range o.opens {
				if op[0]%1000 == 300 {
					return
				}
			}
			t.Fatalf("no open resumed at the byte a reset reached: %v", o.opens)
		})
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// Refused connections are retried up to the schedule, then reported.
func TestStreamRetriesRefusedOpens(t *testing.T) {
	o := &fakeObject{data: randomBytes(5000)}
	o.hook = func(call int, off, n int64, r io.Reader) (io.Reader, error) {
		if call < 6 {
			return nil, syscall.ECONNREFUSED
		}
		return r, nil
	}
	var out bytes.Buffer
	require.NoError(t, Stream(context.Background(), &out, o.source(), StreamOptions{ChunkSize: 1000, Window: 2, Retry: fastRetry}))
	assert.True(t, bytes.Equal(o.data, out.Bytes()))

	o = &fakeObject{data: randomBytes(5000)}
	o.hook = func(call int, off, n int64, r io.Reader) (io.Reader, error) {
		if off >= 2000 {
			return nil, syscall.ECONNREFUSED
		}
		return r, nil
	}
	out.Reset()
	err := Stream(context.Background(), &out, o.source(), StreamOptions{ChunkSize: 1000, Window: 2, Retry: fastRetry})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "after 4 attempt(s)")
	assert.ErrorIs(t, err, syscall.ECONNREFUSED)
	assert.True(t, bytes.Equal(o.data[:2000], out.Bytes()), "everything before the failure, in order, and nothing after")
}

// A response that ends early without an error is a failure, not the end.
func TestStreamTreatsAShortBodyAsAFailure(t *testing.T) {
	o := &fakeObject{data: randomBytes(5000)}
	o.hook = func(call int, off, n int64, r io.Reader) (io.Reader, error) {
		return io.LimitReader(r, 10), nil
	}
	var out bytes.Buffer
	require.NoError(t, Stream(context.Background(), &out, o.source(), StreamOptions{ChunkSize: 1000, Window: 2, Retry: fastRetry}))
	assert.True(t, bytes.Equal(o.data, out.Bytes()), "progress resets the attempts, so a trickle completes")

	o.hook = func(call int, off, n int64, r io.Reader) (io.Reader, error) {
		if off >= 1000 {
			return bytes.NewReader(nil), nil
		}
		return r, nil
	}
	out.Reset()
	err := Stream(context.Background(), &out, o.source(), StreamOptions{ChunkSize: 1000, Window: 2, Retry: fastRetry})
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestStreamStopsAtAPermanentError(t *testing.T) {
	o := &fakeObject{data: randomBytes(5000)}
	gone := errors.New("412 precondition failed")
	o.hook = func(call int, off, n int64, r io.Reader) (io.Reader, error) {
		return nil, gone
	}
	src := o.source()
	src.Permanent = func(err error) bool { return errors.Is(err, gone) }
	err := Stream(context.Background(), io.Discard, src, StreamOptions{ChunkSize: 1000, Window: 2, Retry: []time.Duration{time.Hour}})
	assert.ErrorIs(t, err, gone)
	assert.Contains(t, err.Error(), "after 1 attempt(s)")
}

// The bytes are out before the checksum is known; a mismatch is still an error.
func TestStreamReportsAChecksumMismatchAfterWriting(t *testing.T) {
	o := &fakeObject{data: randomBytes(5000)}
	src := o.source()
	src.CRC32C++
	var out bytes.Buffer
	err := Stream(context.Background(), &out, src, StreamOptions{ChunkSize: 1000, Window: 2, Retry: fastRetry})
	assert.ErrorIs(t, err, ErrStreamCorrupt)
	assert.Equal(t, 5000, out.Len())

	sum := md5.Sum(o.data)
	src = o.source()
	src.HasCRC32C, src.MD5 = false, sum[:]
	assert.NoError(t, Stream(context.Background(), io.Discard, src, StreamOptions{ChunkSize: 1000, Retry: fastRetry}))
	src.MD5 = append([]byte{}, sum[:]...)
	src.MD5[0]++
	assert.ErrorIs(t, Stream(context.Background(), io.Discard, src, StreamOptions{ChunkSize: 1000, Retry: fastRetry}), ErrStreamCorrupt)

	// a source that sends other bytes than it promised
	o.hook = func(call int, off, n int64, r io.Reader) (io.Reader, error) {
		b, _ := io.ReadAll(r)
		b[0] ^= 1
		return bytes.NewReader(b), nil
	}
	assert.ErrorIs(t, Stream(context.Background(), io.Discard, o.source(), StreamOptions{ChunkSize: 1000, Retry: fastRetry}), ErrStreamCorrupt)
}

// A closed stdout stops everything: one error, and no goroutine left behind,
// even with fetchers blocked mid-read.
func TestStreamStopsCleanlyWhenTheWriterFails(t *testing.T) {
	before := runtime.NumGoroutine()
	o := &fakeObject{data: randomBytes(1 << 20)}
	src := o.source()
	open := src.Open
	src.Open = func(ctx context.Context, off, n int64) (io.ReadCloser, error) {
		rc, err := open(ctx, off, n)
		if err != nil || off == 0 {
			return rc, err
		}
		// every chunk after the first stalls until cancelled
		return struct {
			io.Reader
			io.Closer
		}{readerFunc(func([]byte) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		}), rc}, nil
	}
	var writes atomic.Int32
	w := writerFunc(func(p []byte) (int, error) {
		writes.Add(1)
		return 0, syscall.EPIPE
	})
	start := time.Now()
	err := Stream(context.Background(), w, src, StreamOptions{ChunkSize: 4096, Window: 8, Retry: DefaultStreamRetry})
	assert.True(t, IsStreamWriteError(err))
	assert.ErrorIs(t, err, syscall.EPIPE)
	assert.Equal(t, int32(1), writes.Load())
	assert.Less(t, time.Since(start), 2*time.Second)
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.LessOrEqual(t, runtime.NumGoroutine(), before)
	assert.Equal(t, int32(0), o.inFlight.Load(), "every response body is closed")
}

// A writer that takes part of a write and reports no error ends the stream as
// a write failure. Retrying it as a read would reopen past the bytes w never
// took.
func TestStreamStopsAtAShortWrite(t *testing.T) {
	for _, chunk := range []int64{0, 4096} {
		t.Run(fmt.Sprintf("chunk=%d", chunk), func(t *testing.T) {
			// larger than one sequential read, so a reopen would land mid-object
			o := &fakeObject{data: randomBytes(3 * sequentialReadSize)}
			var out bytes.Buffer
			var writes atomic.Int32
			w := writerFunc(func(p []byte) (int, error) {
				if writes.Add(1) == 1 {
					half := len(p) / 2
					out.Write(p[:half])
					return half, nil
				}
				return out.Write(p)
			})
			err := Stream(context.Background(), w, o.source(), StreamOptions{ChunkSize: chunk, Window: 4, Retry: fastRetry})
			require.Error(t, err)
			assert.True(t, IsStreamWriteError(err), "got %v", err)
			assert.ErrorIs(t, err, io.ErrShortWrite)
			assert.Equal(t, int32(1), writes.Load())
			assert.True(t, bytes.HasPrefix(o.data, out.Bytes()), "what was written is a prefix of the object")
		})
	}
}

// A response that stops delivering without an error, as a lost one does, is
// abandoned after the stall timeout and reopened where it stopped.
func TestStreamReopensAStalledRead(t *testing.T) {
	for _, chunk := range []int64{1000, 0} {
		t.Run(fmt.Sprintf("chunk=%d", chunk), func(t *testing.T) {
			o := &fakeObject{data: randomBytes(5000)}
			src := o.source()
			open := src.Open
			var stalls atomic.Int32
			var lead atomic.Int64
			lead.Store(200)
			src.Open = func(ctx context.Context, off, n int64) (io.ReadCloser, error) {
				rc, err := open(ctx, off, n)
				if err != nil || stalls.Load() >= 2 {
					return rc, err
				}
				stalls.Add(1)
				return struct {
					io.Reader
					io.Closer
				}{io.MultiReader(io.LimitReader(rc, lead.Load()), readerFunc(func([]byte) (int, error) {
					<-ctx.Done()
					return 0, ctx.Err()
				})), rc}, nil
			}
			var out bytes.Buffer
			err := Stream(context.Background(), &out, src, StreamOptions{ChunkSize: chunk, Window: 2, Retry: fastRetry, Stall: 50 * time.Millisecond})
			require.NoError(t, err)
			assert.True(t, bytes.Equal(o.data, out.Bytes()))

			// one that never recovers fails, saying why
			stalls.Store(-1000)
			lead.Store(0)
			err = Stream(context.Background(), io.Discard, src, StreamOptions{ChunkSize: chunk, Window: 2, Retry: fastRetry, Stall: 20 * time.Millisecond})
			assert.ErrorIs(t, err, errStalled)
		})
	}
}

// A consumer slower than the stall timeout does not make the read look stalled.
func TestStreamSlowConsumerIsNotAStall(t *testing.T) {
	o := &fakeObject{data: randomBytes(4 << 20)}
	w := writerFunc(func(p []byte) (int, error) {
		time.Sleep(30 * time.Millisecond)
		return len(p), nil
	})
	require.NoError(t, Stream(context.Background(), w, o.source(), StreamOptions{ChunkSize: 0, Retry: fastRetry, Stall: 10 * time.Millisecond}))
	o.mu.Lock()
	defer o.mu.Unlock()
	assert.Len(t, o.opens, 1, "the one stream was never reopened")
}
