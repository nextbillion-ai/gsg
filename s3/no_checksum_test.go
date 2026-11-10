package s3

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"

	"github.com/nextbillion-ai/gsg/bar"
	"github.com/nextbillion-ai/gsg/system"
	"github.com/nextbillion-ai/gsg/worker"
)

// attributeReplies maps an object's key to what GetObjectAttributes returns.
func attributeReplies(t *testing.T) *S3 {
	t.Helper()
	body := func(checksum string) string {
		return `<GetObjectAttributesResponse><ObjectSize>5</ObjectSize>` + checksum + `</GetObjectAttributesResponse>`
	}
	replies := map[string]string{
		"aws-cp.bin":    body(`<Checksum><ChecksumCRC64NVME>tzbvBuYbmUU=</ChecksumCRC64NVME><ChecksumType>FULL_OBJECT</ChecksumType></Checksum>`),
		"composite.bin": body(`<Checksum><ChecksumCRC32C>lu/2iQ==-2</ChecksumCRC32C><ChecksumType>COMPOSITE</ChecksumType></Checksum>`),
		"none.bin":      body(``),
		"malformed.bin": body(`<Checksum><ChecksumCRC32C>!!notbase64</ChecksumCRC32C><ChecksumType>FULL_OBJECT</ChecksumType></Checksum>`),
		"gsg.bin":       body(`<Checksum><ChecksumCRC32C>` + crc32cToBase64(7) + `</ChecksumCRC32C><ChecksumType>FULL_OBJECT</ChecksumType></Checksum>`),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		reply, ok := replies[key]
		switch {
		case key == "gone.bin":
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `<Error><Code>NoSuchKey</Code></Error>`)
		case !ok:
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `<Error><Code>AccessDenied</Code></Error>`)
		default:
			fmt.Fprint(w, reply)
		}
	}))
	t.Cleanup(srv.Close)
	return &S3{clients: map[string]*s3.Client{"b": clientAt(srv)}}
}

func TestListedObjectSaysWhetherItStoresAChecksum(t *testing.T) {
	s := attributeReplies(t)
	keys := []string{"aws-cp.bin", "composite.bin", "none.bin", "malformed.bin", "gsg.bin", "denied.bin", "gone.bin"}
	batch := &crcBatch{s: s, bucket: "b", keys: keys}
	at := time.Unix(1700000000, 0)
	want := map[string]bool{
		"aws-cp.bin":    true,  // stores a CRC64NVME
		"composite.bin": true,  // a CRC32C of parts cannot be compared with a file
		"none.bin":      true,  // stores nothing
		"malformed.bin": false, // stores one gsg cannot read: unknown, not absent
		"gsg.bin":       false, // has one
		"denied.bin":    false, // the lookup failed: unknown, not absent
		"gone.bin":      false, // vanished: unknown, not absent
	}
	for _, key := range keys {
		attrs := s.toAttrs(s.listedAttrs("b", types.Object{Key: aws.String(key), Size: aws.Int64(5), LastModified: &at}, batch))
		assert.Equal(t, want[key], attrs.NoCRC32C(), key)
	}
}

func TestObjectFromGetObjectAttributesSaysWhetherItStoresAChecksum(t *testing.T) {
	s := &S3{}
	toAttrs := func(c *types.Checksum) bool {
		return s.toAttrs(&S3Attributes{S3Attrs: &s3.GetObjectAttributesOutput{Checksum: c}}).NoCRC32C()
	}
	assert.True(t, toAttrs(nil))
	assert.True(t, toAttrs(&types.Checksum{ChecksumCRC64NVME: aws.String("tzbvBuYbmUU="), ChecksumType: types.ChecksumTypeFullObject}))
	assert.True(t, toAttrs(&types.Checksum{ChecksumCRC32C: aws.String("lu/2iQ==-2"), ChecksumType: types.ChecksumTypeComposite}))
	assert.False(t, toAttrs(&types.Checksum{ChecksumCRC32C: aws.String("!!notbase64"), ChecksumType: types.ChecksumTypeFullObject}))
	assert.False(t, toAttrs(&types.Checksum{ChecksumCRC32C: aws.String(crc32cToBase64(7)), ChecksumType: types.ChecksumTypeFullObject}))
}

// Reading whether objects store a checksum costs no more than reading the
// checksums: the two share one lookup per object.
func TestReadingBothAnswersCostsOneLookupPerObject(t *testing.T) {
	var lookups atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		lookups.Add(1)
		fmt.Fprint(w, `<GetObjectAttributesResponse><ObjectSize>5</ObjectSize></GetObjectAttributesResponse>`)
	}))
	defer srv.Close()
	s := &S3{clients: map[string]*s3.Client{"b": clientAt(srv)}}
	keys := []string{"a", "b", "c"}
	batch := &crcBatch{s: s, bucket: "b", keys: keys}
	at := time.Unix(1700000000, 0)
	for _, key := range keys {
		attrs := s.toAttrs(s.listedAttrs("b", types.Object{Key: aws.String(key), Size: aws.Int64(5), LastModified: &at}, batch))
		attrs.NoCRC32C()
		attrs.CalcCRC32C()
		attrs.NoCRC32C()
	}
	assert.Equal(t, int32(3), lookups.Load())
}

// rangedObject serves one object, honouring byte ranges the way S3 does, and can
// end a response early without an error, which a Content-Length would prevent.
func rangedObject(t *testing.T, content []byte, shortBy int) *S3 {
	t.Helper()
	ranged := regexp.MustCompile(`bytes=(\d+)-(\d+)`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("attributes") {
			w.Header().Set("Last-Modified", "Sun, 05 Nov 2026 02:14:22 GMT")
			fmt.Fprintf(w, `<GetObjectAttributesResponse><ObjectSize>%d</ObjectSize></GetObjectAttributesResponse>`, len(content))
			return
		}
		if len(content) == 0 {
			if r.Header.Get("Range") != "" {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				fmt.Fprint(w, `<Error><Code>InvalidRange</Code></Error>`)
			}
			return
		}
		m := ranged.FindStringSubmatch(r.Header.Get("Range"))
		start, _ := strconv.Atoi(m[1])
		end, _ := strconv.Atoi(m[2])
		end = min(end, len(content)-1)
		part := content[start : end+1]
		w.WriteHeader(http.StatusPartialContent)
		w.(http.Flusher).Flush() // chunked, so no length to hold the body to
		_, _ = w.Write(part[:max(len(part)-shortBy, 0)])
	}))
	t.Cleanup(srv.Close)
	return &S3{clients: map[string]*s3.Client{"b": clientAt(srv)}}
}

func download(t *testing.T, s *S3, chunkSize int64, dst string) error {
	t.Helper()
	return downloadWith(t, s, chunkSize, false, dst)
}

func downloadWith(t *testing.T, s *S3, chunkSize int64, gentle bool, dst string) error {
	t.Helper()
	bars, err := bar.New()
	assert.NoError(t, err)
	pool := worker.New(2, false)
	pool.Run()
	defer pool.Close()
	return s.Download("b", "o.bin", dst, false, system.RunContext{Pool: pool, Concurrency: 2, Bars: bars, ChunkSize: chunkSize, GentleIO: gentle})
}

func TestDownloadAcceptsCompleteChunks(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789"), 100)
	for _, chunkSize := range []int64{0, 64, 333, 1000} {
		dst := filepath.Join(t.TempDir(), "o.bin")
		assert.NoError(t, download(t, rangedObject(t, content, 0), chunkSize, dst), "chunk size %d", chunkSize)
		got, err := os.ReadFile(dst)
		assert.NoError(t, err)
		assert.Equal(t, content, got, "chunk size %d", chunkSize)
	}
}

// Left alone this would be a file of the right size, zeroes at the end, carrying
// the object's time: for an object with no checksum, skipped by rsync for good.
func TestDownloadRefusesAChunkThatEndsEarly(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789"), 100)
	for _, chunkSize := range []int64{0, 333} {
		dir := t.TempDir()
		dst := filepath.Join(dir, "o.bin")
		err := download(t, rangedObject(t, content, 10), chunkSize, dst)
		assert.Error(t, err, "chunk size %d", chunkSize)
		_, statErr := os.Stat(dst)
		assert.True(t, os.IsNotExist(statErr), "nothing may be installed, chunk size %d", chunkSize)
		left, _ := os.ReadDir(dir)
		assert.Empty(t, left, "the temp file is removed, chunk size %d", chunkSize)
	}
}

func TestGentleDownloadChecksChunksToo(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789"), 100)
	dst := filepath.Join(t.TempDir(), "o.bin")
	assert.NoError(t, downloadWith(t, rangedObject(t, content, 0), 333, true, dst))
	got, err := os.ReadFile(dst)
	assert.NoError(t, err)
	assert.Equal(t, content, got)

	dst = filepath.Join(t.TempDir(), "o.bin")
	assert.Error(t, downloadWith(t, rangedObject(t, content, 10), 333, true, dst))
	_, statErr := os.Stat(dst)
	assert.True(t, os.IsNotExist(statErr))
}

// S3 answers any range on an empty object with 416, which ended the whole run.
func TestDownloadOfAnEmptyObjectAsksForNoRange(t *testing.T) {
	for _, gentle := range []bool{false, true} {
		dst := filepath.Join(t.TempDir(), "o.bin")
		assert.NoError(t, downloadWith(t, rangedObject(t, nil, 0), 0, gentle, dst))
		info, err := os.Stat(dst)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), info.Size())
	}
}
