package s3

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/nextbillion-ai/gsg/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The client gsg builds fails a download whose body stops, and the SDK's own
// retries are bounded, so the error comes back.
func TestDownloadFailsWhenTheBodyStalls(t *testing.T) {
	defer func(d time.Duration) { common.StallTimeout = d }(common.StallTimeout)
	common.StallTimeout = 200 * time.Millisecond
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("attributes") {
			w.Header().Set("Last-Modified", "Sun, 05 Nov 2026 02:14:22 GMT")
			fmt.Fprint(w, `<GetObjectAttributesResponse><ObjectSize>1000000</ObjectSize></GetObjectAttributesResponse>`)
			return
		}
		w.Header().Set("Content-Length", "1000000")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(make([]byte, 4096))
		w.(http.Flusher).Flush()
		<-release
	}))
	defer func() {
		close(release)
		srv.Close()
	}()
	c := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  aws.AnonymousCredentials{},
		HTTPClient:   newHTTPClient(),
	})
	s := &S3{clients: map[string]*s3.Client{"b": c}}
	for _, gentle := range []bool{false, true} {
		start := time.Now()
		err := downloadWith(t, s, 0, gentle, filepath.Join(t.TempDir(), "o.bin"))
		require.Error(t, err)
		var se *common.StallError
		assert.True(t, errors.As(err, &se), "got %v", err)
		assert.Less(t, time.Since(start), 5*time.Second)
	}
}

func TestRegionLookupTransportIsGuarded(t *testing.T) {
	defer func(d time.Duration) { common.StallTimeout = d }(common.StallTimeout)
	common.StallTimeout = 200 * time.Millisecond
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer func() {
		close(release)
		srv.Close()
	}()
	resp, err := (&http.Client{Transport: newTransport()}).Get(srv.URL)
	if resp != nil {
		_ = resp.Body.Close()
	}
	var se *common.StallError
	assert.True(t, errors.As(err, &se), "got %v", err)
}
