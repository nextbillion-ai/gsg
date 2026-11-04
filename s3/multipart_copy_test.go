package s3

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/nextbillion-ai/gsg/common"
)

const sixGiB int64 = 6 * 1024 * 1024 * 1024

// fakeS3 answers just enough of the protocol to drive Copy, and records what it
// was asked. Sizes are only ever reported, so a multi-GiB object costs nothing.
type fakeS3 struct {
	t    *testing.T
	size int64
	// wholeCRC, when set, is reported as the source's FULL_OBJECT CRC32C.
	wholeCRC *uint32
	tags     map[string]string
	// partResponse, when set, overrides the answer to a part with this number.
	partResponse map[int]func(w http.ResponseWriter)
	// abortsToClear is how many aborts the upload survives being listed after.
	abortsToClear int

	mu        sync.Mutex
	parts     []fakePart
	creates   []*http.Request
	completes []string
	copies    []*http.Request
	aborts    int
}

type fakePart struct {
	number  int
	source  string
	rng     string
	ifMatch string
}

// partCRC is the checksum the fake reports for a part. Distinct per part, so an
// assembly in the wrong order folds to something else.
func partCRC(n int) uint32 { return uint32(n)*2654435761 + 7 }

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	_, _ = io.Copy(io.Discard, r.Body)
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodGet && q.Has("attributes"):
		fmt.Fprintf(w, `<GetObjectAttributesResponse><ObjectSize>%d</ObjectSize></GetObjectAttributesResponse>`, f.size)
	case r.Method == http.MethodHead:
		h := w.Header()
		h.Set("Content-Length", strconv.FormatInt(f.size, 10))
		h.Set("ETag", `"source-etag"`)
		h.Set("Content-Type", "application/x-tar")
		h.Set("Cache-Control", "max-age=60")
		h.Set("x-amz-meta-origin", "fake")
		if f.wholeCRC != nil {
			h.Set("x-amz-checksum-crc32c", crc32cToBase64(*f.wholeCRC))
			h.Set("x-amz-checksum-type", "FULL_OBJECT")
		}
	case r.Method == http.MethodGet && q.Has("tagging"):
		var b strings.Builder
		b.WriteString(`<Tagging><TagSet>`)
		for k, v := range f.tags {
			fmt.Fprintf(&b, `<Tag><Key>%s</Key><Value>%s</Value></Tag>`, k, v)
		}
		b.WriteString(`</TagSet></Tagging>`)
		_, _ = io.WriteString(w, b.String())
	case r.Method == http.MethodPost && q.Has("uploads"):
		f.creates = append(f.creates, r)
		_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><UploadId>up-1</UploadId></InitiateMultipartUploadResult>`)
	case r.Method == http.MethodPut && q.Has("partNumber"):
		n, _ := strconv.Atoi(q.Get("partNumber"))
		f.parts = append(f.parts, fakePart{
			number:  n,
			source:  r.Header.Get("x-amz-copy-source"),
			rng:     r.Header.Get("x-amz-copy-source-range"),
			ifMatch: r.Header.Get("x-amz-copy-source-if-match"),
		})
		if respond := f.partResponse[n]; respond != nil {
			respond(w)
			return
		}
		fmt.Fprintf(w, `<CopyPartResult><ETag>"part-%d"</ETag><ChecksumCRC32C>%s</ChecksumCRC32C></CopyPartResult>`, n, crc32cToBase64(partCRC(n)))
	case r.Method == http.MethodPut && r.Header.Get("x-amz-copy-source") != "":
		f.copies = append(f.copies, r)
		_, _ = io.WriteString(w, `<CopyObjectResult><ETag>"copied"</ETag></CopyObjectResult>`)
	case r.Method == http.MethodPost && q.Has("uploadId"):
		f.completes = append(f.completes, r.Header.Get("x-amz-checksum-crc32c")+"|"+r.Header.Get("x-amz-checksum-type")+"|"+r.Header.Get("x-amz-mp-object-size"))
		_, _ = io.WriteString(w, `<CompleteMultipartUploadResult><ETag>"done"</ETag></CompleteMultipartUploadResult>`)
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		f.aborts++
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodGet && q.Has("uploadId"):
		// How an abort is confirmed. The upload goes once it has been aborted
		// abortsToClear times.
		if f.aborts < f.abortsToClear || f.aborts == 0 {
			_, _ = io.WriteString(w, `<ListPartsResult><Part><PartNumber>1</PartNumber></Part></ListPartsResult>`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `<Error><Code>NoSuchUpload</Code><Message>gone</Message></Error>`)
	default:
		f.t.Errorf("fake s3: unexpected %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func clientAt(srv *httptest.Server) *s3.Client {
	return s3.New(s3.Options{
		Region:           "us-east-1",
		BaseEndpoint:     aws.String(srv.URL),
		UsePathStyle:     true,
		Credentials:      aws.AnonymousCredentials{},
		RetryMaxAttempts: 1,
	})
}

// newCopyFixture returns an S3 whose "src" and "dst" buckets both reach f.
func newCopyFixture(t *testing.T, f *fakeS3) *S3 {
	t.Helper()
	f.t = t
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c := clientAt(srv)
	return &S3{clients: map[string]*s3.Client{"src": c, "dst": c}}
}

// foldedParts is the whole-object checksum of n fake parts covering size bytes.
func foldedParts(size, partSize int64, n int) uint32 {
	sums := make([]uint32, n)
	lens := make([]int64, n)
	for i := 0; i < n; i++ {
		sums[i] = partCRC(i + 1)
		lens[i] = partSize
		if i == n-1 {
			lens[i] = size - int64(i)*partSize
		}
	}
	crc, _ := common.FoldCRC32C(sums, lens)
	return crc
}

func TestCopyAtTheLimitStaysASingleCopyObject(t *testing.T) {
	f := &fakeS3{size: s3MaxCopySource}
	s := newCopyFixture(t, f)
	if err := s.Copy("src", "a.tar", "dst", "b.tar"); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if len(f.copies) != 1 || len(f.creates) != 0 {
		t.Fatalf("exactly 5 GiB is what CopyObject accepts: got %d CopyObject and %d multipart", len(f.copies), len(f.creates))
	}
}

func TestCopyOverTheLimitGoesInParts(t *testing.T) {
	size := s3MaxCopySource + 1
	f := &fakeS3{size: size, tags: map[string]string{"team": "routing"}}
	s := newCopyFixture(t, f)
	if err := s.Copy("src", "dir/plus+sign café.tar", "dst", "b.tar"); err != nil {
		t.Fatalf("Copy: %v", err)
	}

	if len(f.copies) != 0 {
		t.Errorf("CopyObject refuses this size, and was called %d times", len(f.copies))
	}
	partSize, parts := common.PartGeometry(size, -1, s3MinPartSize, s3MaxPartSize)
	if len(f.parts) != int(parts) {
		t.Fatalf("got %d parts, want %d", len(f.parts), parts)
	}
	sort.Slice(f.parts, func(a, b int) bool { return f.parts[a].number < f.parts[b].number })

	// Contiguous, from the first byte to the last: this is what makes the
	// result the same object rather than most of it.
	re := regexp.MustCompile(`^bytes=(\d+)-(\d+)$`)
	var next int64
	for i, p := range f.parts {
		if p.number != i+1 {
			t.Fatalf("part %d numbered %d", i+1, p.number)
		}
		m := re.FindStringSubmatch(p.rng)
		if m == nil {
			t.Fatalf("part %d range %q", p.number, p.rng)
		}
		from, _ := strconv.ParseInt(m[1], 10, 64)
		to, _ := strconv.ParseInt(m[2], 10, 64)
		if from != next {
			t.Fatalf("part %d starts at %d, want %d", p.number, from, next)
		}
		if p.number < int(parts) && to-from+1 != partSize {
			t.Fatalf("part %d has %d bytes, want %d", p.number, to-from+1, partSize)
		}
		next = to + 1
		if p.ifMatch != `"source-etag"` {
			t.Errorf("part %d is not pinned to the source's ETag: %q", p.number, p.ifMatch)
		}
		// The exact header, not its decoding: an unencoded key decodes to
		// itself as well, and that is the bug this guards against.
		if want := "src/dir/plus%2Bsign%20caf%C3%A9.tar"; p.source != want {
			t.Errorf("part %d source %q, want %q", p.number, p.source, want)
		}
	}
	if next != size {
		t.Fatalf("parts cover %d bytes of %d", next, size)
	}

	// What CopyObject keeps on its own.
	if len(f.creates) != 1 {
		t.Fatalf("%d creates", len(f.creates))
	}
	h := f.creates[0].Header
	for name, want := range map[string]string{
		"Content-Type":        "application/x-tar",
		"Cache-Control":       "max-age=60",
		"X-Amz-Meta-Origin":   "fake",
		"X-Amz-Tagging":       "team=routing",
		"X-Amz-Checksum-Type": "FULL_OBJECT",
	} {
		if got := h.Get(name); got != want {
			t.Errorf("create %s = %q, want %q", name, got, want)
		}
	}

	if len(f.completes) != 1 || f.aborts != 0 {
		t.Fatalf("%d completes, %d aborts", len(f.completes), f.aborts)
	}
	want := fmt.Sprintf("%s|FULL_OBJECT|%d", crc32cToBase64(foldedParts(size, partSize, int(parts))), size)
	if f.completes[0] != want {
		t.Errorf("complete = %q, want %q", f.completes[0], want)
	}
}

// The complete request must list its parts in order, or the service rejects it.
func TestCopyCompletesWithPartsInOrder(t *testing.T) {
	var body string
	f := &fakeS3{size: sixGiB}
	mux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("uploadId") {
			b, _ := io.ReadAll(r.Body)
			body = string(b)
		}
		f.ServeHTTP(w, r)
	})
	f.t = t
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := clientAt(srv)
	s := &S3{clients: map[string]*s3.Client{"src": c, "dst": c}}
	if err := s.Copy("src", "a.tar", "dst", "b.tar"); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	var doc struct {
		Parts []struct {
			PartNumber int
			ETag       string
		} `xml:"Part"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("complete body: %v\n%s", err, body)
	}
	_, parts := common.PartGeometry(sixGiB, -1, s3MinPartSize, s3MaxPartSize)
	if len(doc.Parts) != int(parts) {
		t.Fatalf("complete lists %d parts, want %d", len(doc.Parts), parts)
	}
	for i, p := range doc.Parts {
		if p.PartNumber != i+1 || p.ETag != fmt.Sprintf(`"part-%d"`, i+1) {
			t.Fatalf("entry %d is part %d etag %s", i, p.PartNumber, p.ETag)
		}
	}
}

func TestCopyAbortsRatherThanPublishes(t *testing.T) {
	wrong := uint32(0xdeadbeef)
	for name, f := range map[string]*fakeS3{
		// The source changed under the copy.
		"precondition failed": {size: sixGiB, partResponse: map[int]func(http.ResponseWriter){
			3: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code><Message>no</Message></Error>`)
			},
		}},
		// A copy can answer 200 and report its failure in the body.
		"error inside a 200": {size: sixGiB, partResponse: map[int]func(http.ResponseWriter){
			3: func(w http.ResponseWriter) {
				_, _ = io.WriteString(w, `<Error><Code>AccessDenied</Code><Message>no</Message></Error>`)
			},
		}},
		"no checksum on a part": {size: sixGiB, partResponse: map[int]func(http.ResponseWriter){
			3: func(w http.ResponseWriter) {
				_, _ = io.WriteString(w, `<CopyPartResult><ETag>"p"</ETag></CopyPartResult>`)
			},
		}},
		"checksum differs from the source": {size: sixGiB, wholeCRC: &wrong},
	} {
		t.Run(name, func(t *testing.T) {
			s := newCopyFixture(t, f)
			if err := s.Copy("src", "a.tar", "dst", "b.tar"); err == nil {
				t.Fatal("Copy reported success")
			}
			if len(f.completes) != 0 {
				t.Errorf("published anyway: %d completes", len(f.completes))
			}
			if f.aborts != 1 {
				t.Errorf("%d aborts, want 1: the parts bill until this happens", f.aborts)
			}
		})
	}
}

// An abort does not always take the first time; the parts bill until it does.
func TestCopyRepeatsAnAbortUntilTheUploadIsGone(t *testing.T) {
	defer func(d time.Duration) { abortCopyBackoff = d }(abortCopyBackoff)
	abortCopyBackoff = time.Millisecond

	f := &fakeS3{size: sixGiB, abortsToClear: 3, partResponse: map[int]func(http.ResponseWriter){
		3: func(w http.ResponseWriter) { w.WriteHeader(http.StatusForbidden) },
	}}
	s := newCopyFixture(t, f)
	if err := s.Copy("src", "a.tar", "dst", "b.tar"); err == nil {
		t.Fatal("Copy reported success")
	}
	if f.aborts != 3 {
		t.Errorf("%d aborts, want 3", f.aborts)
	}
}

// An upload that will not go must not hold the worker forever, and the error the
// caller sees stays the one that failed the copy.
func TestCopyGivesUpOnAnAbortThatNeverTakes(t *testing.T) {
	defer func(d time.Duration) { abortCopyBackoff = d }(abortCopyBackoff)
	abortCopyBackoff = time.Millisecond

	f := &fakeS3{size: sixGiB, abortsToClear: 1000, partResponse: map[int]func(http.ResponseWriter){
		3: func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code><Message>no</Message></Error>`)
		},
	}}
	s := newCopyFixture(t, f)
	err := s.Copy("src", "a.tar", "dst", "b.tar")
	if err == nil || !strings.Contains(err.Error(), "PreconditionFailed") {
		t.Fatalf("Copy returned %v, want the part's failure", err)
	}
	if f.aborts != abortCopyAttempts {
		t.Errorf("%d aborts, want %d", f.aborts, abortCopyAttempts)
	}
}

func TestAbortCopyStopsAtItsDeadline(t *testing.T) {
	defer func(a int, b, d time.Duration) {
		abortCopyAttempts, abortCopyBackoff, abortCopyDeadline = a, b, d
	}(abortCopyAttempts, abortCopyBackoff, abortCopyDeadline)
	abortCopyAttempts, abortCopyBackoff, abortCopyDeadline = 100, time.Hour, 50*time.Millisecond

	f := &fakeS3{t: t, size: sixGiB, abortsToClear: 1000}
	srv := httptest.NewServer(f)
	defer srv.Close()

	start := time.Now()
	abortCopy(context.Background(), clientAt(srv), "dst", "b.tar", aws.String("up-1"))
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("took %s against a 50ms deadline", took)
	}
	if f.aborts != 1 {
		t.Errorf("%d aborts: the backoff should have been cut short by the deadline", f.aborts)
	}
}

// Dropping the connection does not stop the service working on a part, so the
// abort has to come after the parts in flight have finished, not race them.
func TestCopyLetsPartsInFlightFinishBeforeAborting(t *testing.T) {
	part2Started := make(chan struct{})
	var mu sync.Mutex
	var part2Done, part2Cancelled, abortedEarly bool

	f := &fakeS3{t: t, size: sixGiB, partResponse: map[int]func(http.ResponseWriter){
		1: func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code><Message>no</Message></Error>`)
		},
	}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case q.Get("partNumber") == "1":
			<-part2Started // fail only once part 2 is known to be in flight
		case q.Get("partNumber") == "2":
			close(part2Started)
			select {
			case <-r.Context().Done():
				mu.Lock()
				part2Cancelled = true
				mu.Unlock()
			case <-time.After(300 * time.Millisecond):
			}
			mu.Lock()
			part2Done = true
			mu.Unlock()
		case r.Method == http.MethodDelete:
			mu.Lock()
			abortedEarly = !part2Done
			mu.Unlock()
		}
		f.ServeHTTP(w, r)
	}))
	defer srv.Close()
	c := clientAt(srv)
	s := &S3{clients: map[string]*s3.Client{"src": c, "dst": c}}

	if err := s.Copy("src", "a.tar", "dst", "b.tar"); err == nil {
		t.Fatal("Copy reported success")
	}
	mu.Lock()
	defer mu.Unlock()
	if part2Cancelled {
		t.Error("the part in flight was cancelled when another failed")
	}
	if abortedEarly {
		t.Error("the abort went out while a part was still in flight")
	}
	if f.aborts != 1 {
		t.Errorf("%d aborts, want 1", f.aborts)
	}
}

func TestCopyAcceptsASourceChecksumThatAgrees(t *testing.T) {
	partSize, parts := common.PartGeometry(sixGiB, -1, s3MinPartSize, s3MaxPartSize)
	whole := foldedParts(sixGiB, partSize, int(parts))
	f := &fakeS3{size: sixGiB, wholeCRC: &whole}
	s := newCopyFixture(t, f)
	if err := s.Copy("src", "a.tar", "dst", "b.tar"); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if len(f.completes) != 1 {
		t.Fatalf("%d completes", len(f.completes))
	}
}

func TestCopyOntoItselfIsRefused(t *testing.T) {
	f := &fakeS3{size: sixGiB}
	s := newCopyFixture(t, f)
	if err := s.Copy("src", "a.tar", "src", "a.tar"); err == nil {
		t.Fatal("Copy onto itself reported success")
	}
	if len(f.creates) != 0 {
		t.Errorf("%d multipart uploads started", len(f.creates))
	}
}

// The buckets may be in different regions, so each request has to go to the
// right one. This is how a copy across regions went wrong before (#46).
func TestCopyTalksToEachBucketsOwnClient(t *testing.T) {
	var srcHits, dstHits []string
	var mu sync.Mutex
	src := &fakeS3{t: t, size: sixGiB}
	dst := &fakeS3{t: t, size: sixGiB}
	srcSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		srcHits = append(srcHits, r.Method+" "+r.URL.RawQuery)
		mu.Unlock()
		src.ServeHTTP(w, r)
	}))
	defer srcSrv.Close()
	dstSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		dstHits = append(dstHits, r.Method+" "+r.URL.RawQuery)
		mu.Unlock()
		dst.ServeHTTP(w, r)
	}))
	defer dstSrv.Close()

	s := &S3{clients: map[string]*s3.Client{"src": clientAt(srcSrv), "dst": clientAt(dstSrv)}}
	if err := s.Copy("src", "a.tar", "dst", "b.tar"); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if len(src.creates) != 0 || len(src.parts) != 0 || len(src.completes) != 0 {
		t.Errorf("the write requests went to the source's client: %v", srcHits)
	}
	if len(dst.creates) != 1 || len(dst.completes) != 1 {
		t.Errorf("the destination's client saw: %v", dstHits)
	}
	for _, hit := range dstHits {
		if strings.HasPrefix(hit, "HEAD") || strings.Contains(hit, "tagging") {
			t.Errorf("a read of the source went to the destination's client: %s", hit)
		}
	}
}

func TestCopySourceHeaderIsEncoded(t *testing.T) {
	for _, tc := range []struct{ key, want string }{
		{"plain.tar", "b/plain.tar"},
		{"dir/sub/plain.tar", "b/dir/sub/plain.tar"},
		{"with space.tar", "b/with%20space.tar"},
		// decodes to a space if left alone, naming another key
		{"plus+sign.tar", "b/plus%2Bsign.tar"},
		// decodes to a space if left alone, naming another key
		{"a%20b.tar", "b/a%2520b.tar"},
		{"café.tar", "b/caf%C3%A9.tar"},
		{"run=2026-08-23T02:00:00+05:30/x", "b/run%3D2026-08-23T02%3A00%3A00%2B05%3A30/x"},
		// a raw one would end the key, or start a query string
		{"what?.tar", "b/what%3F.tar"},
		{"hash#tag.tar", "b/hash%23tag.tar"},
		{"semi;colon.tar", "b/semi%3Bcolon.tar"},
		// the slashes are the key's structure, and every one of them is kept
		{"a//b.tar", "b/a//b.tar"},
		{"/leading.tar", "b//leading.tar"},
		{"trailing/", "b/trailing/"},
	} {
		if got := copySourceHeader("b", tc.key); got != tc.want {
			t.Errorf("copySourceHeader(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}
}
