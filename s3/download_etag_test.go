package s3

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
)

// An object overwritten between two chunks must fail the download, not leave
// a file made of bytes from both versions.
func TestDownloadPinsETagAcrossChunks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", "Sun, 05 Nov 2026 02:14:22 GMT")
		if r.URL.Query().Has("attributes") {
			fmt.Fprint(w, `<GetObjectAttributesResponse><ObjectSize>8</ObjectSize><ETag>"old"</ETag></GetObjectAttributesResponse>`)
			return
		}
		fields := strings.Split(strings.TrimPrefix(r.Header.Get("Range"), "bytes="), "-")
		start, _ := strconv.Atoi(fields[0])
		end, _ := strconv.Atoi(fields[1])
		if end > 7 {
			end = 7
		}
		// The object is replaced after the first chunk has been served.
		etag, data := `"old"`, "AAAAAAAA"
		if start > 0 {
			etag, data = `"new"`, "BBBBBBBB"
		}
		if pin := r.Header.Get("If-Match"); pin != "" && pin != etag {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/8", start, end))
		w.WriteHeader(http.StatusPartialContent)
		fmt.Fprint(w, data[start:end+1])
	}))
	defer srv.Close()

	s := &S3{clients: map[string]*s3.Client{"b": clientAt(srv)}}
	dst := filepath.Join(t.TempDir(), "mixed")
	err := download(t, s, 4, dst)

	assert.Error(t, err)
	_, statErr := os.Stat(dst)
	assert.True(t, os.IsNotExist(statErr), "no final file should be left behind")
}
