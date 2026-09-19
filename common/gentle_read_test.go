package common

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingBar is what a progress bar looks like to this package: something
// that can be written to, and that can be wound back when a body is rewound.
type countingBar struct{ n int64 }

func (c *countingBar) Write(p []byte) (int, error) { c.n += int64(len(p)); return len(p), nil }
func (c *countingBar) IncrBy(delta int64)          { c.n += delta }

func sectionFixture(t *testing.T, size int) (*os.File, []byte) {
	t.Helper()
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i*13 + i/97)
	}
	path := filepath.Join(t.TempDir(), "src.bin")
	require.NoError(t, os.WriteFile(path, content, 0o644))
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	return f, content
}

func withRecordedReadPacing(t *testing.T) (*[]time.Duration, *[]dropRequest) {
	t.Helper()
	var slept []time.Duration
	var dropped []dropRequest
	sleep, advise := gentleSleep, gentleAdviseDropRead
	gentleSleep = func(d time.Duration) { slept = append(slept, d) }
	gentleAdviseDropRead = func(_ *os.File, offset, length int64) {
		dropped = append(dropped, dropRequest{offset, length})
	}
	t.Cleanup(func() { gentleSleep, gentleAdviseDropRead = sleep, advise })
	return &slept, &dropped
}

// Whatever else it does, it has to hand back exactly the bytes of its section
// -- gentle or not, and from an offset.
func TestGentleSectionReadsItsOwnBytes(t *testing.T) {
	const size = 3*GentleWindow + 777
	f, content := sectionFixture(t, size)

	for _, gentle := range []Gentle{{}, {Pause: true, Drop: true}} {
		for _, span := range [][2]int64{{0, size}, {0, GentleWindow}, {GentleWindow + 5, 2 * GentleWindow}, {size - 10, 10}} {
			off, length := span[0], span[1]
			g := NewGentleSection(f, off, length, gentle, nil)
			got, err := io.ReadAll(g)
			require.NoError(t, err)
			assert.Equal(t, content[off:off+length], got, "gentle=%+v off=%d len=%d", gentle, off, length)
			assert.Equal(t, length, g.Size())
		}
	}
}

// Without --gentle-io nothing is advised and nothing sleeps: the wrapper is
// then only there so the body still counts bytes and still seeks.
func TestGentleSectionPacesNothingWhenNotAskedTo(t *testing.T) {
	slept, dropped := withRecordedReadPacing(t)
	f, _ := sectionFixture(t, 3*GentleWindow)
	_, err := io.Copy(io.Discard, NewGentleSection(f, 0, 3*GentleWindow, Gentle{}, nil))
	require.NoError(t, err)
	assert.Empty(t, *slept)
	assert.Empty(t, *dropped)
}

// Under --gentle-io every byte read has to be dropped, including the tail, and
// the pauses have to come at the same rate the write side uses.
func TestGentleSectionDropsEveryByteItReadsAndPacesThem(t *testing.T) {
	const size = 3*GentleWindow + 1234
	const off = 4096
	slept, dropped := withRecordedReadPacing(t)
	f, _ := sectionFixture(t, off+size)

	g := NewGentleSection(f, off, size, Gentle{Pause: true, Drop: true}, nil)
	_, err := io.Copy(io.Discard, g)
	require.NoError(t, err)

	// Contiguous, inside the section, and covering all of it exactly once:
	// a read leaves clean pages, so unlike the write side one request per
	// range is enough and a second would be waste.
	var covered int64
	for _, d := range *dropped {
		assert.Equal(t, off+covered, d.offset, "drop requests must be contiguous: %v", *dropped)
		covered += d.length
	}
	assert.Equal(t, int64(size), covered, "every byte read is dropped, the tail included")

	var total time.Duration
	for _, d := range *slept {
		total += d
	}
	assert.Equal(t, gentlePause*time.Duration(size)/GentleWindow, total,
		"the rate must not depend on how much each Read returned")
}

// The body of an upload has to stay rewindable, or the SDK turns its own retry
// off -- which is what an io.TeeReader did on the path the CLI always takes.
func TestGentleSectionSeeksAndWindsTheBarBack(t *testing.T) {
	const size = 2 * GentleWindow
	f, content := sectionFixture(t, size)
	bar := &countingBar{}
	g := NewGentleSection(f, 0, size, Gentle{Pause: true, Drop: true}, bar)

	var _ io.ReadSeeker = g // the SDK reflects for exactly this

	half := make([]byte, size/2)
	_, err := io.ReadFull(g, half)
	require.NoError(t, err)
	assert.Equal(t, int64(size/2), bar.n)

	// The retry: back to the start, and the bar with it.
	at, err := g.Seek(0, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(0), at)
	assert.Equal(t, int64(0), bar.n, "a rewound body must not count its bytes twice")

	all, err := io.ReadAll(g)
	require.NoError(t, err)
	assert.Equal(t, content, all, "the whole section is readable again after a rewind")
	assert.Equal(t, int64(size), bar.n)
}

// A rewind re-reads pages that were dropped on purpose, so the accounting has
// to start over too -- otherwise the second pass drops nothing and the file
// stays in the cache.
func TestGentleSectionDropsAgainAfterARewind(t *testing.T) {
	const size = 2 * GentleWindow
	_, dropped := withRecordedReadPacing(t)
	f, _ := sectionFixture(t, size)

	g := NewGentleSection(f, 0, size, Gentle{Pause: true, Drop: true}, nil)
	_, err := io.Copy(io.Discard, g)
	require.NoError(t, err)
	first := len(*dropped)
	require.NotZero(t, first)

	_, err = g.Seek(0, io.SeekStart)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, g)
	require.NoError(t, err)

	var covered int64
	for _, d := range (*dropped)[first:] {
		covered += d.length
	}
	assert.Equal(t, int64(size), covered, "the second pass drops what it read as well")
}

// Reading through a tiny buffer must not change the rate or leave a range
// undropped: the window is counted in bytes, not in Read calls.
func TestGentleSectionDoesNotDependOnTheReadSize(t *testing.T) {
	const size = GentleWindow + 3
	slept, dropped := withRecordedReadPacing(t)
	f, content := sectionFixture(t, size)

	g := NewGentleSection(f, 0, size, Gentle{Pause: true, Drop: true}, nil)
	var got bytes.Buffer
	_, err := io.CopyBuffer(&got, struct{ io.Reader }{g}, make([]byte, 5000))
	require.NoError(t, err)
	assert.Equal(t, content, got.Bytes())

	var covered int64
	for _, d := range *dropped {
		covered += d.length
	}
	assert.Equal(t, int64(size), covered)

	var total time.Duration
	for _, d := range *slept {
		total += d
	}
	assert.Equal(t, gentlePause*time.Duration(size)/GentleWindow, total)
}

// Pause and Drop are separate because an upload wants them in different
// places: the pause on the checksum read, which is the one that goes to the
// disk, and the drop on the send, which reads the pages that read just filled.
func TestGentlePauseAndDropAreIndependent(t *testing.T) {
	const size = 2*GentleWindow + 11

	slept, dropped := withRecordedReadPacing(t)
	f, _ := sectionFixture(t, size)
	_, err := io.Copy(io.Discard, NewGentleSection(f, 0, size, Gentle{Pause: true}, nil))
	require.NoError(t, err)
	assert.NotEmpty(t, *slept, "Pause must pace")
	assert.Empty(t, *dropped, "Pause must not drop: the send still has to read these pages")

	slept, dropped = withRecordedReadPacing(t)
	f, _ = sectionFixture(t, size)
	_, err = io.Copy(io.Discard, NewGentleSection(f, 0, size, Gentle{Drop: true}, nil))
	require.NoError(t, err)
	assert.Empty(t, *slept, "Drop must not pace: the read it follows already did")
	var covered int64
	for _, d := range *dropped {
		covered += d.length
	}
	assert.Equal(t, int64(size), covered, "Drop must cover every byte read")
}

// The tail, through a real request rather than a mock of one, because the
// question is what an HTTP client actually does with a known-length body.
//
// It was raised in review as a defect -- that net/http reads exactly
// ContentLength bytes and never comes back for the io.EOF, leaving every
// body's last window cached. Measured, that is not so: net/http copies
// through an io.LimitReader and then reads once more to check for extra
// bytes, so the EOF does arrive. This passes with the length-based release
// removed.
//
// It is kept, and so is that release, because the body is handed to the SDK
// wrapped and the wrapper decides how it is read. "The last window is dropped"
// should not rest on a consumer making a read it does not need.
func TestGentleSectionReleasesItsTailUnderARealRequest(t *testing.T) {
	for _, size := range []int{GentleWindow / 4, GentleWindow + 1234, 2 * GentleWindow} {
		slept, dropped := withRecordedReadPacing(t)
		f, content := sectionFixture(t, size)

		var got int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n, _ := io.Copy(io.Discard, r.Body)
			got = n
			w.WriteHeader(http.StatusOK)
		}))

		body := NewGentleSection(f, 0, int64(size), Gentle{Pause: true, Drop: true}, nil)
		req, err := http.NewRequest(http.MethodPut, srv.URL, io.NopCloser(body))
		require.NoError(t, err)
		req.ContentLength = int64(size)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_, _ = io.Copy(io.Discard, resp.Body)
		require.NoError(t, resp.Body.Close())
		srv.Close()

		require.Equal(t, int64(len(content)), got, "size %d: the server got the whole body", size)

		var covered int64
		for _, d := range *dropped {
			covered += d.length
		}
		assert.Equal(t, int64(size), covered, "size %d: every byte sent must be dropped, the tail included", size)

		var total time.Duration
		for _, d := range *slept {
			total += d
		}
		assert.Equal(t, gentlePause*time.Duration(size)/GentleWindow, total, "size %d: paced at the usual rate", size)
	}
}
