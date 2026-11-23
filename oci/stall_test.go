package oci

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ResponseHeaderTimeout covers neither a body that stops mid-way nor an upload
// the service stops taking; the stall guard covers both.
func TestHTTPClientFailsAStalledBodyAndAStalledUpload(t *testing.T) {
	defer func(d time.Duration) { common.StallTimeout = d }(common.StallTimeout)
	common.StallTimeout = 200 * time.Millisecond
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			_, _ = io.CopyN(io.Discard, r.Body, 1<<20)
			<-release
			return
		}
		w.Header().Set("Content-Length", "1000000")
		_, _ = w.Write(make([]byte, 4096))
		w.(http.Flusher).Flush()
		<-release
	}))
	defer func() {
		close(release)
		srv.Close()
	}()
	c := newHTTPClient()
	var se *common.StallError

	resp, err := c.Get(srv.URL)
	require.NoError(t, err)
	start := time.Now()
	_, err = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	assert.True(t, errors.As(err, &se), "got %v", err)
	assert.Less(t, time.Since(start), 5*time.Second)

	req, err := http.NewRequest(http.MethodPut, srv.URL, bytes.NewReader(make([]byte, 256<<20)))
	require.NoError(t, err)
	start = time.Now()
	resp, err = c.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	assert.True(t, errors.As(err, &se), "got %v", err)
	assert.Less(t, time.Since(start), 5*time.Second)
}
