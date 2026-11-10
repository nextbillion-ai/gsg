package s3

import (
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A temp file that cannot be created must fail the download with the reason.
// Each chunk used to open it with the error dropped and seek the nil handle,
// so a destination the process may not write to surfaced as "seek for offset
// with invalid argument", after the first range read had already gone out.
func TestDownloadIntoAnUnwritableDestinationSaysWhy(t *testing.T) {
	var reads atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("attributes") {
			w.Header().Set("Last-Modified", "Sun, 05 Nov 2026 02:14:22 GMT")
			fmt.Fprint(w, `<GetObjectAttributesResponse><ObjectSize>8</ObjectSize></GetObjectAttributesResponse>`)
			return
		}
		reads.Add(1)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("01234"))
	}))
	t.Cleanup(srv.Close)
	s := &S3{clients: map[string]*s3.Client{"b": clientAt(srv)}}

	dir := t.TempDir()
	notDir := filepath.Join(dir, "file")
	require.NoError(t, os.WriteFile(notDir, nil, 0o644))
	cases := []struct {
		name string
		dst  string
		want error
	}{{"parent is a file", filepath.Join(notDir, "o.bin"), syscall.ENOTDIR}}
	// A read-only directory stops only a user other than root, which is how
	// the goroute image runs against a volume root owned by root.
	if os.Geteuid() != 0 {
		ro := filepath.Join(dir, "ro")
		require.NoError(t, os.Mkdir(ro, 0o555))
		cases = append(cases, struct {
			name string
			dst  string
			want error
		}{"read-only directory", filepath.Join(ro, "o.bin"), fs.ErrPermission})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reads.Store(0)
			require.ErrorIs(t, download(t, s, 4, tc.dst), tc.want)
			assert.Zero(t, reads.Load(), "nothing is fetched for a file that has nowhere to go")
		})
	}
}

// A temp file created but not sized is removed with the failure.
func TestDownloadRemovesATempFileItCouldNotSize(t *testing.T) {
	const huge = int64(1) << 62
	dir := t.TempDir()
	probe := filepath.Join(dir, "probe")
	f, err := os.Create(probe)
	require.NoError(t, err)
	terr := f.Truncate(huge)
	_ = f.Close()
	require.NoError(t, os.Remove(probe))
	if terr == nil {
		t.Skip("this filesystem allocates a sparse file of 4 EiB")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", "Sun, 05 Nov 2026 02:14:22 GMT")
		fmt.Fprintf(w, `<GetObjectAttributesResponse><ObjectSize>%d</ObjectSize></GetObjectAttributesResponse>`, huge)
	}))
	t.Cleanup(srv.Close)
	s := &S3{clients: map[string]*s3.Client{"b": clientAt(srv)}}

	require.Error(t, download(t, s, 4, filepath.Join(dir, "o.bin")))
	left, _ := os.ReadDir(dir)
	assert.Empty(t, left)
}
