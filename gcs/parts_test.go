package gcs

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

type listedObject struct {
	name       string
	generation int64
	created    time.Time
}

// fakeBucket lists objects (every version, filtered by prefix) and records
// deletes as name@generation. A name in refuse is answered 403.
type fakeBucket struct {
	mu      sync.Mutex
	objects []listedObject
	refuse  map[string]bool
	deleted []string
	queries []string
}

func (fb *fakeBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	q := r.URL.Query()
	switch r.Method {
	case http.MethodGet:
		fb.mu.Lock()
		fb.queries = append(fb.queries, r.URL.RawQuery)
		type item struct {
			Name        string `json:"name"`
			Generation  string `json:"generation"`
			TimeCreated string `json:"timeCreated"`
		}
		items := []item{}
		for _, o := range fb.objects {
			if len(o.name) >= len(q.Get("prefix")) && o.name[:len(q.Get("prefix"))] == q.Get("prefix") {
				items = append(items, item{o.name, strconv.FormatInt(o.generation, 10), o.created.Format(time.RFC3339Nano)})
			}
		}
		fb.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"kind": "storage#objects", "items": items})
	case http.MethodDelete:
		name := path.Base(r.URL.Path)
		if fb.refuse[name] {
			http.Error(w, `{"error":{"code":403,"message":"held"}}`, http.StatusForbidden)
			return
		}
		fb.mu.Lock()
		fb.deleted = append(fb.deleted, name+"@"+q.Get("generation"))
		fb.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unexpected", http.StatusBadRequest)
	}
}

func bucketClient(t *testing.T, fb *fakeBucket) *GCS {
	t.Helper()
	srv := httptest.NewServer(fb)
	t.Cleanup(srv.Close)
	client, err := storage.NewClient(context.Background(), option.WithEndpoint(srv.URL), option.WithoutAuthentication())
	require.NoError(t, err)
	return &GCS{client: client}
}

func fixClock(t *testing.T, now time.Time) {
	t.Helper()
	old := sweepNow
	sweepNow = func() time.Time { return now }
	t.Cleanup(func() { sweepNow = old })
}

// A killed upload's parts go, every version at the generation listed, while
// the parts of an upload still running beside it stay, its old part included:
// one young part is enough to show it is alive.
func TestSweepPartsRemovesDeadUploadsAndLeavesALiveOneAlone(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	fixClock(t, now)
	old, young := now.Add(-3*time.Hour), now.Add(-time.Minute)
	fb := &fakeBucket{objects: []listedObject{
		// killed gsg upload of dir/a.bin, with a noncurrent version of one part
		{"dir/a.bin.gsg-part-00aa11bb22cc33dd-00", 11, old},
		{"dir/a.bin.gsg-part-00aa11bb22cc33dd-00", 12, old.Add(time.Second)},
		{"dir/a.bin.gsg-part-00aa11bb22cc33dd-01", 13, old},
		// a running upload of the same object: its first part finished long ago
		{"dir/a.bin.gsg-part-feedfacecafebeef-00", 21, old},
		{"dir/a.bin.gsg-part-feedfacecafebeef-01", 22, young},
		// killed jam-core upload
		{"dir/b.csv.jam-part-0123456789abcdef-03", 31, old},
		// not parts
		{"dir/a.bin", 41, old},
		{"dir/notes.gsg-part-readme", 42, old},
		{"dir/c.gsg-part-xyz-00", 43, old},
	}}
	g := bucketClient(t, fb)

	res, err := g.SweepParts(context.Background(), "b", "dir/", time.Hour)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"dir/a.bin.gsg-part-00aa11bb22cc33dd-00#11",
		"dir/a.bin.gsg-part-00aa11bb22cc33dd-00#12",
		"dir/a.bin.gsg-part-00aa11bb22cc33dd-01#13",
		"dir/b.csv.jam-part-0123456789abcdef-03#31",
	}, res.Removed)
	assert.Equal(t, []string{
		"dir/a.bin.gsg-part-feedfacecafebeef-00#21",
		"dir/a.bin.gsg-part-feedfacecafebeef-01#22",
	}, res.Live)
	assert.Empty(t, res.Left)
	sort.Strings(fb.deleted)
	assert.Equal(t, []string{
		"a.bin.gsg-part-00aa11bb22cc33dd-00@11",
		"a.bin.gsg-part-00aa11bb22cc33dd-00@12",
		"a.bin.gsg-part-00aa11bb22cc33dd-01@13",
		"b.csv.jam-part-0123456789abcdef-03@31",
	}, fb.deleted)
	require.NotEmpty(t, fb.queries)
	assert.Contains(t, fb.queries[0], "versions=true", "noncurrent versions of parts are found too")
}

func TestSweepPartsReportsWhatItCouldNotDelete(t *testing.T) {
	now := time.Now()
	fixClock(t, now)
	fb := &fakeBucket{
		objects: []listedObject{
			{"a.gsg-part-0011-00", 1, now.Add(-2 * time.Hour)},
			{"a.gsg-part-0011-01", 2, now.Add(-2 * time.Hour)},
		},
		refuse: map[string]bool{"a.gsg-part-0011-01": true},
	}
	res, err := bucketClient(t, fb).SweepParts(context.Background(), "b", "", time.Hour)
	require.NoError(t, err)
	assert.Equal(t, []string{"a.gsg-part-0011-00#1"}, res.Removed)
	assert.Equal(t, []string{"a.gsg-part-0011-01#2"}, res.Left)
}

func TestSweepPartsNeedsAPositiveAge(t *testing.T) {
	fb := &fakeBucket{objects: []listedObject{{"a.gsg-part-0011-00", 1, time.Now()}}}
	g := bucketClient(t, fb)
	for _, age := range []time.Duration{0, -time.Hour} {
		_, err := g.SweepParts(context.Background(), "b", "", age)
		assert.Error(t, err)
	}
	assert.Empty(t, fb.deleted)
}

// The sweep a composite upload runs first is for its own object: parts of
// another object whose name extends it are not its to judge.
func TestSweepStalePartsKeepsToItsObject(t *testing.T) {
	now := time.Now()
	fixClock(t, now)
	old := now.Add(-2 * DefaultStalePartAge)
	fb := &fakeBucket{objects: []listedObject{
		{"o.gsg-part-0a0b-00", 1, old},
		{"o.x.gsg-part-0c0d-00", 2, old},
		{"o2.gsg-part-0e0f-00", 3, old},
		{"o.jam-part-1a1b-05", 4, old},
		{"o.gsg-part-2a2b-00", 5, now.Add(-time.Minute)},
	}}
	bucketClient(t, fb).sweepStaleParts("b", "o")
	sort.Strings(fb.deleted)
	assert.Equal(t, []string{"o.gsg-part-0a0b-00@1", "o.jam-part-1a1b-05@4"}, fb.deleted)
}

// A composite upload lists its object's parts before it writes its own.
func TestUploadCompositeSweepsItsObjectFirst(t *testing.T) {
	fc := &fakeComposite{}
	f, fi := compositeSource(t)
	require.NoError(t, runComposite(t, fc, f, fi))
	assert.Equal(t, 1, fc.listed)
}
