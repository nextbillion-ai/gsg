package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

// guardedClient is a client on gsg's own transport with the stall limit set to
// stall, against srv.
func guardedClient(t *testing.T, srv *httptest.Server, stall time.Duration) *GCS {
	t.Helper()
	defer func(d time.Duration) { common.StallTimeout = d }(common.StallTimeout)
	common.StallTimeout = stall
	client, err := storage.NewClient(context.Background(), option.WithEndpoint(srv.URL), option.WithoutAuthentication(),
		option.WithHTTPClient(&http.Client{Transport: NewTransport(false)}))
	require.NoError(t, err)
	return &GCS{client: client}
}

// stallingServer serves one object's metadata, and hands every other request
// to h, which may block until the test ends.
func stallingServer(t *testing.T, size int, h func(w http.ResponseWriter, r *http.Request, release <-chan struct{})) *httptest.Server {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("alt") == "json" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"bucket":"b","name":"o","size":"%d","generation":"7","crc32c":"AAAAAA=="}`, size)
			return
		}
		h(w, r, release)
	}))
	t.Cleanup(func() {
		close(release)
		srv.Close()
	})
	return srv
}

func requireFinalStall(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var se *common.StallError
	assert.True(t, errors.As(err, &se), "got %v", err)
	// the library retries what it calls retryable for as long as the context
	// lives, which for gsg is forever
	assert.False(t, storage.ShouldRetry(err), "the library would retry %v", err)
}

// A download whose body stops must fail as an error the library does not retry
// by itself. The library reopens a broken read once from where it stopped; that
// request stalls too, before its headers.
func TestDownloadFailsWhenTheBodyStalls(t *testing.T) {
	for _, gentle := range []bool{false, true} {
		t.Run(fmt.Sprintf("gentle=%t", gentle), func(t *testing.T) {
			srv := stallingServer(t, 1<<20, func(w http.ResponseWriter, r *http.Request, release <-chan struct{}) {
				var start int
				_, _ = fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &start)
				if start > 0 {
					<-release // the reopen
					return
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", 1<<20-1, 1<<20))
				w.Header().Set("Content-Length", fmt.Sprint(1<<20))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(make([]byte, 4096))
				w.(http.Flusher).Flush()
				<-release
			})
			g := guardedClient(t, srv, 300*time.Millisecond)
			dst := filepath.Join(t.TempDir(), "o")
			start := time.Now()
			err := g.Download("b", "o", dst, true, system.RunContext{ChunkSize: 0, GentleIO: gentle})
			requireFinalStall(t, err)
			assert.Less(t, time.Since(start), 5*time.Second)
			assert.NoFileExists(t, dst)
			assert.NoFileExists(t, common.GetTempFile(dst))
		})
	}
}

func TestDownloadFailsWhenTheHeadersStall(t *testing.T) {
	srv := stallingServer(t, 1<<20, func(w http.ResponseWriter, r *http.Request, release <-chan struct{}) {
		<-release
	})
	g := guardedClient(t, srv, 300*time.Millisecond)
	start := time.Now()
	err := g.Download("b", "o", filepath.Join(t.TempDir(), "o"), true, system.RunContext{ChunkSize: 256 << 10, Concurrency: 4})
	requireFinalStall(t, err)
	assert.Less(t, time.Since(start), 5*time.Second)
}

// cp and rsync retry a transfer three times; a stall is one of the errors they
// retry, and the third ends it rather than the library going on forever.
func TestStalledDownloadEndsAtTheCommandsRetries(t *testing.T) {
	var requests atomic.Int64
	srv := stallingServer(t, 1<<20, func(w http.ResponseWriter, r *http.Request, release <-chan struct{}) {
		requests.Add(1)
		<-release
	})
	g := guardedClient(t, srv, 200*time.Millisecond)
	start := time.Now()
	err := common.DoTransferWithRetry(func() error {
		return g.Download("b", "o", filepath.Join(t.TempDir(), "o"), true, system.RunContext{ChunkSize: 0})
	})
	requireFinalStall(t, err)
	// Per attempt, net/http replays the GET once, because it went out on the
	// connection the metadata lookup had used; the library adds none.
	assert.EqualValues(t, 6, requests.Load())
	assert.Less(t, time.Since(start), 5*time.Second)
}

// A body that keeps moving downloads however long it takes in total.
func TestDownloadSlowerThanTheStallLimitInTotalSucceeds(t *testing.T) {
	content := []byte(strings.Repeat("0123456789", 100))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("alt") == "json" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"bucket":"b","name":"o","size":"%d","generation":"7","crc32c":"%s"}`, len(content), crc32cBase64(content))
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(content)-1, len(content)))
		w.Header().Set("Content-Length", fmt.Sprint(len(content)))
		w.WriteHeader(http.StatusPartialContent)
		for i := 0; i < len(content); i += 100 {
			_, _ = w.Write(content[i : i+100])
			w.(http.Flusher).Flush()
			time.Sleep(50 * time.Millisecond)
		}
	}))
	defer srv.Close()
	g := guardedClient(t, srv, 200*time.Millisecond)
	dst := filepath.Join(t.TempDir(), "o")
	start := time.Now()
	require.NoError(t, g.Download("b", "o", dst, true, system.RunContext{ChunkSize: 0, GentleIO: true}))
	assert.Greater(t, time.Since(start), 400*time.Millisecond)
	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	assert.Equal(t, content, got)
}

// A service that stops taking an upload part-way leaves the client's writes
// blocked once the socket buffers are full; the upload fails, and nothing is
// committed.
func TestUploadFailsWhenTheServiceStopsTakingIt(t *testing.T) {
	var commits atomic.Int64
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("uploadType") == "resumable" {
			w.Header().Set("Location", "http://"+r.Host+"/session")
			return
		}
		if r.URL.Path == "/session" {
			_, _ = io.CopyN(io.Discard, r.Body, 1<<20)
			<-release
			commits.Add(1)
			return
		}
		http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusBadRequest)
	}))
	defer func() {
		close(release)
		srv.Close()
	}()
	g := guardedClient(t, srv, 300*time.Millisecond)
	src := filepath.Join(t.TempDir(), "big")
	f, err := os.Create(src)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(64<<20))
	require.NoError(t, f.Close())

	start := time.Now()
	err = g.Upload(src, "b", "o", system.RunContext{})
	requireFinalStall(t, err)
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Zero(t, commits.Load())
}

// A compose the service never answers fails the upload at composeTimeout, and
// the parts are still deleted. The client here has no stall guard, as a
// caller's own client may not.
func TestUploadCompositeGivesUpOnAComposeThatHangs(t *testing.T) {
	defer func(d time.Duration) { composeTimeout = d }(composeTimeout)
	composeTimeout = 300 * time.Millisecond
	release := make(chan struct{})
	fc := &fakeComposite{onCompose: func() { <-release }}
	f, fi := compositeSource(t)
	start := time.Now()
	err := runComposite(t, fc, f, fi)
	close(release)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 10*time.Second)
	assert.Len(t, fc.deleted, 2, "the parts are deleted after the compose gave up")
}
