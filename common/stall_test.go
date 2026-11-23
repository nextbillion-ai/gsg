package common

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stallGuardClient(stall time.Duration) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: StallGuardDialContext((&net.Dialer{}).DialContext, stall),
	}}
}

// release unblocks the stalled handlers before httptest.Server.Close waits on them.
func stallServer(t *testing.T, h func(w http.ResponseWriter, r *http.Request, release <-chan struct{})) *httptest.Server {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h(w, r, release) }))
	t.Cleanup(func() {
		close(release)
		srv.Close()
	})
	return srv
}

func requireStall(t *testing.T, err error, op string) {
	t.Helper()
	require.Error(t, err)
	var se *StallError
	require.True(t, errors.As(err, &se), "got %v", err)
	assert.Equal(t, op, se.Op)
	var temporary interface{ Temporary() bool }
	if errors.As(err, &temporary) {
		assert.False(t, temporary.Temporary(), "a stall must not read as temporary: %v", err)
	}
	assert.False(t, errors.Is(err, context.DeadlineExceeded))
}

func TestStallGuardFailsOnStalledHeaders(t *testing.T) {
	srv := stallServer(t, func(w http.ResponseWriter, r *http.Request, release <-chan struct{}) {
		<-release
	})
	start := time.Now()
	resp, err := stallGuardClient(200 * time.Millisecond).Get(srv.URL)
	if resp != nil {
		_ = resp.Body.Close()
	}
	requireStall(t, err, "read")
	assert.Less(t, time.Since(start), 5*time.Second)
}

func TestStallGuardFailsOnStalledBody(t *testing.T) {
	srv := stallServer(t, func(w http.ResponseWriter, r *http.Request, release <-chan struct{}) {
		w.Header().Set("Content-Length", "1000000")
		_, _ = w.Write(make([]byte, 4096))
		w.(http.Flusher).Flush()
		<-release
	})
	resp, err := stallGuardClient(200 * time.Millisecond).Get(srv.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	start := time.Now()
	n, err := io.Copy(io.Discard, resp.Body)
	requireStall(t, err, "read")
	assert.EqualValues(t, 4096, n)
	assert.Less(t, time.Since(start), 5*time.Second)
}

// A server that stops taking an upload leaves the client's write blocked once
// the socket buffers are full.
func TestStallGuardFailsOnAnUploadThePeerStopsTaking(t *testing.T) {
	srv := stallServer(t, func(w http.ResponseWriter, r *http.Request, release <-chan struct{}) {
		_, _ = io.CopyN(io.Discard, r.Body, 1<<20)
		<-release
	})
	start := time.Now()
	resp, err := stallGuardClient(200*time.Millisecond).Post(srv.URL, "application/octet-stream", bytes.NewReader(make([]byte, 256<<20)))
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
	var se *StallError
	require.True(t, errors.As(err, &se), "got %v", err)
	assert.Less(t, time.Since(start), 5*time.Second)
}

// A body that keeps moving may take far longer than the stall limit in total.
func TestStallGuardAllowsSlowSteadyBody(t *testing.T) {
	srv := stallServer(t, func(w http.ResponseWriter, r *http.Request, release <-chan struct{}) {
		for i := 0; i < 10; i++ {
			_, _ = w.Write([]byte("0123456789"))
			w.(http.Flusher).Flush()
			time.Sleep(50 * time.Millisecond)
		}
	})
	resp, err := stallGuardClient(200 * time.Millisecond).Get(srv.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Len(t, b, 100)
}

// The same for an upload the server reads slowly but steadily.
func TestStallGuardAllowsSlowSteadyUpload(t *testing.T) {
	srv := stallServer(t, func(w http.ResponseWriter, r *http.Request, release <-chan struct{}) {
		buf := make([]byte, 64<<10)
		for {
			if _, err := io.ReadFull(r.Body, buf); err != nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
		_, _ = w.Write([]byte("ok"))
	})
	start := time.Now()
	resp, err := stallGuardClient(200*time.Millisecond).Post(srv.URL, "application/octet-stream", bytes.NewReader(make([]byte, 32<<20)))
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Greater(t, time.Since(start), 200*time.Millisecond, "the upload took longer than the stall limit in total")
}

// A connection that sat in the pool must not charge its idle time to the next
// request on it. The request is a POST because the transport would quietly
// retry a GET that failed on a reused connection.
func TestStallGuardPooledConnectionGetsAFullWait(t *testing.T) {
	srv := stallServer(t, func(w http.ResponseWriter, r *http.Request, release <-chan struct{}) {
		if r.URL.Path == "/slow" {
			time.Sleep(250 * time.Millisecond)
		}
		_, _ = w.Write([]byte("ok"))
	})
	c := stallGuardClient(400 * time.Millisecond)
	post := func(p string) error {
		resp, err := c.Post(srv.URL+p, "text/plain", strings.NewReader("x"))
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		_, err = io.ReadAll(resp.Body)
		return err
	}
	require.NoError(t, post("/"))
	time.Sleep(250 * time.Millisecond)
	assert.NoError(t, post("/slow"))
}

func TestStallGuardPassesDialErrors(t *testing.T) {
	want := errors.New("dial failed")
	dial := StallGuardDialContext(func(context.Context, string, string) (net.Conn, error) { return nil, want }, time.Second)
	_, err := dial(context.Background(), "tcp", "127.0.0.1:1")
	assert.ErrorIs(t, err, want)
}

func TestStallGuardOffReturnsTheDialerItWasGiven(t *testing.T) {
	var called bool
	dial := func(context.Context, string, string) (net.Conn, error) { called = true; return nil, io.EOF }
	for _, stall := range []time.Duration{0, -time.Second} {
		called = false
		_, err := StallGuardDialContext(dial, stall)(context.Background(), "tcp", "x")
		assert.ErrorIs(t, err, io.EOF)
		assert.True(t, called)
	}
}

// Once a connection has stalled, whatever fails on it next is that stall, not
// the "use of closed network connection" that the GCS library would retry.
func TestStallGuardReportsTheStallForEveryLaterFailure(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = b.Close() }()
	dial := StallGuardDialContext(func(context.Context, string, string) (net.Conn, error) { return a, nil }, 50*time.Millisecond)
	conn, err := dial(context.Background(), "tcp", "x")
	require.NoError(t, err)

	_, err = conn.Read(make([]byte, 1))
	requireStall(t, err, "read")
	require.NoError(t, conn.Close())

	_, err = conn.Write([]byte("x"))
	requireStall(t, err, "read")
	assert.False(t, errors.Is(err, net.ErrClosed))
	_, err = conn.Read(make([]byte, 1))
	requireStall(t, err, "read")
}

// A connection that failed for any other reason keeps that reason.
func TestStallGuardPassesOtherFailuresThrough(t *testing.T) {
	a, b := net.Pipe()
	dial := StallGuardDialContext(func(context.Context, string, string) (net.Conn, error) { return a, nil }, time.Minute)
	conn, err := dial(context.Background(), "tcp", "x")
	require.NoError(t, err)
	require.NoError(t, b.Close())
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err)
	var se *StallError
	assert.False(t, errors.As(err, &se))
}
