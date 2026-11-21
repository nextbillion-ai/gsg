package s3

import (
	"bytes"
	"fmt"
	"hash/crc32"
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

	"github.com/nextbillion-ai/gsg/bar"
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/system"
	"github.com/nextbillion-ai/gsg/worker"
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

func fullObjectChecksum(crc uint32) string {
	return `<Checksum><ChecksumCRC32C>` + crc32cToBase64(crc) + `</ChecksumCRC32C><ChecksumType>FULL_OBJECT</ChecksumType></Checksum>`
}

func downloadVerified(t *testing.T, s *S3, chunkSize int64, verify, gentle bool, dst string) error {
	t.Helper()
	bars, err := bar.New()
	require.NoError(t, err)
	pool := worker.New(2, false)
	pool.Run()
	defer pool.Close()
	return s.Download("b", "o.bin", dst, verify, system.RunContext{Pool: pool, Concurrency: 2, Bars: bars, ChunkSize: chunkSize, GentleIO: gentle})
}

// cachedCRC32C returns what GetFileCRC32C answers for path once its content no
// longer adds up to it: only a cached value survives that.
func cachedCRC32C(t *testing.T, path string) uint32 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte("x"), int(info.Size())), 0o644))
	require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime()))
	return common.GetFileCRC32C(path)
}

// Every chunk but the last fetches one byte of the next (the range is
// inclusive); that byte must be summed once, by the chunk it belongs to.
func TestDownloadCachesTheCRC32CItSummedWhileWriting(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789"), 100)
	want := crc32.Checksum(content, common.Castagnoli)
	for _, gentle := range []bool{false, true} {
		for _, verify := range []bool{false, true} {
			for _, chunkSize := range []int64{0, 64, 333, 1000} {
				name := fmt.Sprintf("gentle=%v -v=%v chunk=%d", gentle, verify, chunkSize)
				dst := filepath.Join(t.TempDir(), "o.bin")
				require.NoError(t, downloadVerified(t, rangedObjectWithChecksum(t, content, 0, fullObjectChecksum(want)), chunkSize, verify, gentle, dst), name)
				got, err := os.ReadFile(dst)
				require.NoError(t, err)
				assert.Equal(t, content, got, name)
				assert.Equal(t, want, cachedCRC32C(t, dst), name)
			}
		}
	}
}

func TestDownloadWhoseSumDisagreesIsNotCachedAndFailsOnlyUnderV(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789"), 100)
	real := crc32.Checksum(content, common.Castagnoli)
	for _, gentle := range []bool{false, true} {
		name := fmt.Sprintf("gentle=%v", gentle)
		dst := filepath.Join(t.TempDir(), "o.bin")
		assert.Error(t, downloadVerified(t, rangedObjectWithChecksum(t, content, 0, fullObjectChecksum(real^1)), 333, true, gentle, dst), name)

		dst = filepath.Join(t.TempDir(), "o.bin")
		require.NoError(t, downloadVerified(t, rangedObjectWithChecksum(t, content, 0, fullObjectChecksum(real^1)), 333, false, gentle, dst), name)
		got, err := os.ReadFile(dst)
		require.NoError(t, err)
		assert.Equal(t, content, got, name)
		assert.Equal(t, real, common.GetFileCRC32C(dst), "%s: the disagreeing sum is not cached", name)
	}
}

// An object with no whole-object CRC32C -- none at all, or the composite kind a
// multipart upload gets -- has nothing to match, so nothing is cached for it.
func TestDownloadOfAnObjectWithoutACRC32CCachesNothing(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789"), 100)
	real := crc32.Checksum(content, common.Castagnoli)
	composite := `<Checksum><ChecksumCRC32C>` + crc32cToBase64(real) + `-2</ChecksumCRC32C><ChecksumType>COMPOSITE</ChecksumType></Checksum>`
	for _, checksum := range []string{"", composite} {
		for _, verify := range []bool{false, true} {
			dst := filepath.Join(t.TempDir(), "o.bin")
			require.NoError(t, downloadVerified(t, rangedObjectWithChecksum(t, content, 0, checksum), 333, verify, false, dst))
			assert.NotEqual(t, real, cachedCRC32C(t, dst), "checksum %q -v=%v: nothing cached", checksum, verify)
		}
	}
}
