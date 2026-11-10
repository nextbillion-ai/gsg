package common

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultStreamChunkSize is the size of one ranged read when --chunk-size
	// says nothing. It matches what downloads use.
	DefaultStreamChunkSize int64 = 16 * 1024 * 1024
	// DefaultStreamWindow is how many chunks are fetched ahead of the one being
	// written. Memory is window x chunk size: 128 MiB at the defaults.
	DefaultStreamWindow = 8
	// sequentialReadSize is the buffer one unchunked stream copies through.
	sequentialReadSize = 1024 * 1024
)

// DefaultStreamRetry bridges an outage of about 15 s per chunk. The attempts
// count again from the start whenever a chunk makes progress, so a slow link
// that keeps dropping is not failed while it is still delivering.
var DefaultStreamRetry = []time.Duration{
	500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
}

// StreamSource is one version of one object, readable by range.
type StreamSource struct {
	// Name is the object's URL, for messages.
	Name string
	Size int64
	// Open returns n bytes from off, or everything from off when n < 0. It must
	// read the version Size and the checksums describe.
	Open func(ctx context.Context, off, n int64) (io.ReadCloser, error)
	// Permanent reports an error a retry cannot get past. Nil means none is.
	Permanent func(error) bool
	CRC32C    uint32
	HasCRC32C bool
	// MD5 is checked only when there is no CRC32C.
	MD5 []byte
}

// DefaultStreamStall is how long a read may deliver nothing before it is
// abandoned and reopened. A response lost on the way never errors on its own.
const DefaultStreamStall = 30 * time.Second

// StreamOptions shapes a stream. ChunkSize 0 reads the object as one stream,
// negative means DefaultStreamChunkSize. Window < 1 means DefaultStreamWindow.
// Retry nil means DefaultStreamRetry; its length is the number of retries.
// Stall 0 means DefaultStreamStall.
type StreamOptions struct {
	ChunkSize int64
	Window    int
	Retry     []time.Duration
	Stall     time.Duration
}

var errStalled = errors.New("no data arrived")

// ErrStreamCorrupt is returned, wrapped, when every byte was written but the
// bytes do not match the object's checksum or length.
var ErrStreamCorrupt = errors.New("streamed bytes do not match the object")

// writeError marks a failure to write the output, as opposed to read the object.
type writeError struct{ err error }

func (e writeError) Error() string { return fmt.Sprintf("cannot write the output: %s", e.err) }
func (e writeError) Unwrap() error { return e.err }

// IsStreamWriteError reports whether Stream stopped because w refused bytes.
func IsStreamWriteError(err error) bool {
	var we writeError
	return errors.As(err, &we)
}

// Stream writes the object to w in order, as the bytes arrive.
//
// Chunks are fetched in parallel, at most Window of them at a time, each into
// its own buffer; the writer drains the oldest chunk while it is still
// filling, so the first byte goes out as soon as the first response starts.
// A chunk whose read fails is reopened at the byte it reached.
//
// The bytes are already written by the time the checksum can be settled, so a
// mismatch is reported as an error at the end and the caller must exit
// non-zero. Every goroutine Stream started has returned when it returns.
func Stream(ctx context.Context, w io.Writer, src StreamSource, opts StreamOptions) error {
	if opts.Retry == nil {
		opts.Retry = DefaultStreamRetry
	}
	if opts.Window < 1 {
		opts.Window = DefaultStreamWindow
	}
	if opts.Stall <= 0 {
		opts.Stall = DefaultStreamStall
	}
	chunkSize := opts.ChunkSize
	if chunkSize < 0 {
		chunkSize = DefaultStreamChunkSize
	}

	v := newStreamVerifier(src)
	out := io.MultiWriter(writerFunc(func(p []byte) (int, error) {
		n, err := w.Write(p)
		if err == nil && n != len(p) {
			// io.MultiWriter would report this unwrapped, as a read failure
			// to retry past bytes that never reached w.
			err = io.ErrShortWrite
		}
		if err != nil {
			return n, writeError{err}
		}
		return n, nil
	}), v)

	var err error
	switch {
	case src.Size == 0:
	case chunkSize == 0 || chunkSize >= src.Size:
		err = streamSequential(ctx, out, src, opts)
	default:
		err = streamChunked(ctx, out, src, chunkSize, opts)
	}
	if err != nil {
		return err
	}
	return v.settle(src)
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// streamSequential copies the object as one stream, reopened where it stopped.
func streamSequential(ctx context.Context, out io.Writer, src StreamSource, opts StreamOptions) error {
	buf := make([]byte, sequentialReadSize)
	return fetchRange(ctx, src, 0, src.Size, opts,
		func(got int64) []byte { return buf[:min(int64(len(buf)), src.Size-got)] },
		func(p []byte) error {
			_, err := out.Write(p)
			return err
		})
}

// fetchRange reads n bytes from off, handing each read to took, and reopens
// the range at the byte it reached when a read fails or stalls. An error from
// took ends it at once.
func fetchRange(ctx context.Context, src StreamSource, off, n int64, opts StreamOptions,
	next func(got int64) []byte, took func(p []byte) error) error {
	var got int64
	failures := 0
	for got < n {
		before := got
		err := readAttempt(ctx, src, off, n, &got, opts.Stall, next, took)
		if err == nil {
			return nil
		}
		if IsStreamWriteError(err) {
			return err
		}
		if got > before {
			failures = 0
		}
		if failures, err = backoff(ctx, src, err, failures, opts.Retry, off+got); err != nil {
			return err
		}
	}
	return nil
}

// readAttempt is one request for what is left of [off, off+n).
func readAttempt(ctx context.Context, src StreamSource, off, n int64, got *int64, stall time.Duration,
	next func(got int64) []byte, took func(p []byte) error) error {
	actx, cancel := context.WithCancel(ctx)
	defer cancel()
	var stalled atomic.Bool
	timer := time.AfterFunc(stall, func() {
		stalled.Store(true)
		cancel()
	})
	defer timer.Stop()
	explain := func(err error) error {
		if stalled.Load() && ctx.Err() == nil {
			return fmt.Errorf("%w for %s", errStalled, stall)
		}
		return err
	}

	rc, err := src.Open(actx, off+*got, n-*got)
	if err != nil {
		return explain(err)
	}
	defer func() { _ = rc.Close() }()
	for *got < n {
		k, rerr := rc.Read(next(*got))
		if k > 0 {
			// A slow consumer is not a stalled read: the clock runs only
			// while waiting on the response.
			timer.Stop()
			// got moves only past bytes took accepted, so no failure can
			// reopen the range beyond what was written.
			if werr := took(next(*got)[:k]); werr != nil {
				return werr
			}
			*got += int64(k)
			timer.Reset(stall)
		}
		if rerr != nil {
			if *got == n {
				return nil
			}
			if rerr == io.EOF {
				rerr = io.ErrUnexpectedEOF
			}
			return explain(rerr)
		}
	}
	return nil
}

// backoff decides whether a failed read is tried again, and waits if so.
func backoff(ctx context.Context, src StreamSource, err error, failures int, retry []time.Duration, at int64) (int, error) {
	if ctx.Err() != nil {
		return failures, ctx.Err()
	}
	if (src.Permanent != nil && src.Permanent(err)) || failures >= len(retry) {
		return failures, fmt.Errorf("cannot read %s at byte %d after %d attempt(s): %w", src.Name, at, failures+1, err)
	}
	t := time.NewTimer(retry[failures])
	defer t.Stop()
	select {
	case <-ctx.Done():
		return failures, ctx.Err()
	case <-t.C:
	}
	return failures + 1, nil
}

// streamChunk is one range, filled by its fetcher and drained by the writer.
type streamChunk struct {
	off, n int64
	buf    []byte

	mu     sync.Mutex
	cond   sync.Cond
	filled int64
	err    error
}

func streamChunked(ctx context.Context, out io.Writer, src StreamSource, chunkSize int64, opts StreamOptions) error {
	window := opts.Window
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()

	count := (src.Size + chunkSize - 1) / chunkSize
	chunks := make([]*streamChunk, 0, window)
	free := make([][]byte, 0, window)

	start := func(i int64) {
		c := &streamChunk{off: i * chunkSize, n: chunkSize}
		if c.off+c.n > src.Size {
			c.n = src.Size - c.off
		}
		if len(free) > 0 {
			c.buf, free = free[len(free)-1], free[:len(free)-1]
		} else {
			c.buf = make([]byte, chunkSize)
		}
		c.buf = c.buf[:c.n]
		c.cond.L = &c.mu
		chunks = append(chunks, c)
		wg.Add(1)
		go func() {
			defer wg.Done()
			fetchChunk(ctx, src, c, opts)
		}()
	}

	next := int64(0)
	for ; next < count && next < int64(window); next++ {
		start(next)
	}
	for len(chunks) > 0 {
		c := chunks[0]
		var drained int64
		for drained < c.n {
			c.mu.Lock()
			for c.filled == drained && c.err == nil {
				c.cond.Wait()
			}
			filled, err := c.filled, c.err
			c.mu.Unlock()
			if filled > drained {
				if _, werr := out.Write(c.buf[drained:filled]); werr != nil {
					return werr
				}
				drained = filled
				continue
			}
			return err
		}
		chunks = chunks[1:]
		free = append(free, c.buf[:cap(c.buf)])
		if next < count {
			start(next)
			next++
		}
	}
	return nil
}

// fetchChunk fills c and publishes each read to the writer.
func fetchChunk(ctx context.Context, src StreamSource, c *streamChunk, opts StreamOptions) {
	err := fetchRange(ctx, src, c.off, c.n, opts,
		func(got int64) []byte { return c.buf[got:c.n] },
		func(p []byte) error {
			c.mu.Lock()
			c.filled += int64(len(p))
			c.mu.Unlock()
			c.cond.Broadcast()
			return nil
		})
	if err != nil {
		c.mu.Lock()
		c.err = err
		c.mu.Unlock()
		c.cond.Broadcast()
	}
}

// streamVerifier sums what was written.
type streamVerifier struct {
	n   int64
	crc hash.Hash32
	md5 hash.Hash
}

func newStreamVerifier(src StreamSource) *streamVerifier {
	v := &streamVerifier{}
	switch {
	case src.HasCRC32C:
		v.crc = crc32.New(Castagnoli)
	case len(src.MD5) > 0:
		v.md5 = md5.New()
	}
	return v
}

func (v *streamVerifier) Write(p []byte) (int, error) {
	v.n += int64(len(p))
	if v.crc != nil {
		_, _ = v.crc.Write(p)
	}
	if v.md5 != nil {
		_, _ = v.md5.Write(p)
	}
	return len(p), nil
}

func (v *streamVerifier) settle(src StreamSource) error {
	if v.n != src.Size {
		return fmt.Errorf("%w: %s wrote %d bytes, the object has %d", ErrStreamCorrupt, src.Name, v.n, src.Size)
	}
	if v.crc != nil && v.crc.Sum32() != src.CRC32C {
		return fmt.Errorf("%w: %s CRC32C of the output is %d, the object's is %d", ErrStreamCorrupt, src.Name, v.crc.Sum32(), src.CRC32C)
	}
	if v.md5 != nil && !bytes.Equal(v.md5.Sum(nil), src.MD5) {
		return fmt.Errorf("%w: %s MD5 of the output is %x, the object's is %x", ErrStreamCorrupt, src.Name, v.md5.Sum(nil), src.MD5)
	}
	return nil
}

// Verified says which checksum Stream checks for src, for messages.
func (src StreamSource) Verified() string {
	switch {
	case src.HasCRC32C:
		return "CRC32C"
	case len(src.MD5) > 0:
		return "MD5"
	}
	return "length only"
}
