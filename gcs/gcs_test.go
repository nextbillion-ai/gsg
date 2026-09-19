package gcs

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/nextbillion-ai/gsg/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

func TestConfigPath(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "test_path")
	assert.Equal(t, "test_path", ConfigPath())
}

/*
func TestEuqalCRC32C(t *testing.T) {
	g := GCS{}

	assert.True(t, g.equalCRC32C("invalid", "invalid", "invalid"))
	assert.False(t, g.equalCRC32C("gcs.go", "invalid", "invalid"))
	// assert.True(t, equalCRC32C("usa.geojson", "maaas", "borders/usa.geojson"))
	// assert.False(t, equalCRC32C("invalid", "maaas", "borders/usa.geojson"))
}
*/

// A lock cache left short by a run that died mid-write used to panic here with
// "index out of range" when decoded as a uint64. There is no generation to
// match in a short file, so there is nothing it could unlock.
func TestAttemptUnLockIgnoresTruncatedCache(t *testing.T) {
	g := &GCS{}
	const bucket, object = "gsg-test-bucket", "gsg-test/lock-truncated"
	cacheFileName := common.GenTempFileName(bucket, "/", object)
	t.Cleanup(func() { _ = os.Remove(cacheFileName) })

	for _, short := range [][]byte{{}, {0x01}, {0x01, 0x02, 0x03, 0x04}, {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}} {
		assert.NoError(t, os.WriteFile(cacheFileName, short, 0600))
		assert.NotPanics(t, func() {
			// The remote lock stands until its TTL expires, so this must not
			// report a successful unlock.
			assert.Error(t, g.AttemptUnLock(bucket, object), "cache of %d byte(s)", len(short))
		}, "cache of %d byte(s)", len(short))

		// Left in place: another process may have just renamed a valid cache
		// over this path, and the next successful lock replaces it atomically.
		_, err := os.Stat(cacheFileName)
		assert.NoError(t, err, "invalid cache of %d byte(s) must not be removed", len(short))
	}
}

// With no cache at all there is likewise nothing to unlock.
func TestAttemptUnLockWithoutCache(t *testing.T) {
	g := &GCS{}
	const bucket, object = "gsg-test-bucket", "gsg-test/lock-absent"
	_ = os.Remove(common.GenTempFileName(bucket, "/", object))

	assert.NotPanics(t, func() {
		assert.NoError(t, g.AttemptUnLock(bucket, object))
	})
}

// crc32cToSend has to describe the bytes that will actually be uploaded, not
// whatever the path happens to name by then.
//
// It reads the handle for that reason. Consulting the cached checksum for the
// path would be cheaper -- the cache is keyed on path and mtime -- but it is
// then only as good as the assumption that content and mtime move together,
// and GCS rejects the object when they have not. The price of being wrong is a
// whole upload spent to be told so, which for a large file dwarfs the hash.
func TestCRC32CToSendFollowsTheOpenFileNotThePath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "moving.txt")
	original := []byte("the bytes that were opened\n")
	assert.NoError(t, os.WriteFile(path, original, 0600))

	f, err := os.Open(path)
	assert.NoError(t, err)
	defer func() { _ = f.Close() }()

	// Replace the path with different contents, as an atomic writer would.
	replacement := filepath.Join(dir, "replacement.txt")
	assert.NoError(t, os.WriteFile(replacement, []byte("completely different bytes\n"), 0600))
	assert.NoError(t, os.Rename(replacement, path))

	crc, err := crc32cToSend(f, path)
	assert.NoError(t, err)
	assert.Equal(t, crc32.Checksum(original, crc32.MakeTable(crc32.Castagnoli)), crc,
		"the checksum must describe the opened bytes, not the ones now at that path")

	// And the handle must be back at the start, or the upload sends a
	// truncated body that then fails the very check this is for.
	body, err := io.ReadAll(f)
	assert.NoError(t, err)
	assert.Equal(t, original, body)
}

// The ordinary case, where nothing has moved.
func TestCRC32CToSendMatchesTheFileWhenNothingMoved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stable.txt")
	content := []byte("nothing moved here\n")
	assert.NoError(t, os.WriteFile(path, content, 0600))

	f, err := os.Open(path)
	assert.NoError(t, err)
	defer func() { _ = f.Close() }()

	crc, err := crc32cToSend(f, path)
	assert.NoError(t, err)
	assert.Equal(t, crc32.Checksum(content, crc32.MakeTable(crc32.Castagnoli)), crc)

	body, err := io.ReadAll(f)
	assert.NoError(t, err)
	assert.Equal(t, content, body, "the handle is left where the upload needs it")
}

// A file whose contents change while its modification time is put back is what
// makes a path-and-mtime cache lie. Reading the handle is unaffected by it,
// which is the whole reason for reading the handle.
func TestCRC32CToSendIgnoresAStaleModificationTime(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sneaky.txt")
	assert.NoError(t, os.WriteFile(path, []byte("before\n"), 0600))

	fi, err := os.Stat(path)
	assert.NoError(t, err)
	mt := fi.ModTime()

	changed := []byte("afterx\n")
	assert.NoError(t, os.WriteFile(path, changed, 0600))
	assert.NoError(t, os.Chtimes(path, time.Now(), mt))

	f, err := os.Open(path)
	assert.NoError(t, err)
	defer func() { _ = f.Close() }()

	crc, err := crc32cToSend(f, path)
	assert.NoError(t, err)
	assert.Equal(t, crc32.Checksum(changed, crc32.MakeTable(crc32.Castagnoli)), crc,
		"the checksum must describe the current bytes, whatever the mtime says")
}

func TestDeletePartsRetriesOnlyTransientErrorsAndReportsWhatIsLeft(t *testing.T) {
	var mu sync.Mutex
	calls := map[string]int{}
	sleeps := map[time.Duration]int{}
	partDeleteSleep = func(d time.Duration) {
		mu.Lock()
		sleeps[d]++
		mu.Unlock()
	}
	defer func() { partDeleteSleep = time.Sleep }()

	c := &storage.Client{}
	flaky := c.Bucket("b").Object("flaky")     // 503 twice, then deleted
	gone := c.Bucket("b").Object("gone")       // already deleted
	refused := c.Bucket("b").Object("refused") // 403: final at once
	down := c.Bucket("b").Object("down")       // 503 every time
	left := deleteParts([]*storage.ObjectHandle{flaky, nil, gone, refused, down}, func(h *storage.ObjectHandle) error {
		mu.Lock()
		calls[h.ObjectName()]++
		n := calls[h.ObjectName()]
		mu.Unlock()
		switch h.ObjectName() {
		case "flaky":
			if n < 3 {
				return &googleapi.Error{Code: 503}
			}
			return nil
		case "gone":
			return storage.ErrObjectNotExist
		case "refused":
			return &googleapi.Error{Code: 403}
		default:
			return &googleapi.Error{Code: 503}
		}
	})
	assert.Equal(t, []string{"refused", "down"}, left)
	assert.Equal(t, 3, calls["flaky"])
	assert.Equal(t, 1, calls["gone"])
	assert.Equal(t, 1, calls["refused"])
	assert.Equal(t, 3, calls["down"])
	// waits happen between attempts only: two parts made three attempts each
	assert.Equal(t, map[time.Duration]int{partDeleteBackoff: 2, 2 * partDeleteBackoff: 2}, sleeps)
}

func TestPartFailureKeepsThePartThatFailedFirst(t *testing.T) {
	var none partFailure
	_, err := none.get()
	assert.NoError(t, err)

	cause := &googleapi.Error{Code: 503}
	var p partFailure
	p.record(9, cause)
	// the parts cancelled along report later, and not always as context.Canceled
	p.record(3, context.Canceled)
	p.record(0, fmt.Errorf("io: read/write on closed pipe"))
	i, err := p.get()
	assert.Equal(t, 9, i)
	assert.Equal(t, cause, err)
}

func TestSweepLateParts(t *testing.T) {
	c := &storage.Client{}
	obj := func(name string) *storage.ObjectHandle { return c.Bucket("b").Object(name) }
	tests := []struct {
		name string
		// lookup round (0 is at once) from which the part exists; -1 never, -2 lookup refused
		showsAt   map[string]int
		refuseDel map[string]bool
		left      []string
		unchecked []string
		deleted   []string
		waits     int
	}{
		{name: "nothing reached Close: no lookup and no wait", showsAt: map[string]int{}, waits: 0},
		{name: "a part that is there is deleted at once, without a wait", showsAt: map[string]int{"there": 0}, deleted: []string{"there"}, waits: 0},
		{name: "a part committed after the attempt returned is found on a later round", showsAt: map[string]int{"late": 2}, deleted: []string{"late"}, waits: 2},
		{name: "a part that was never committed is looked for until the window closes", showsAt: map[string]int{"never": -1}, waits: int(partSettleWindow / partSettlePoll)},
		{name: "a part that exists and cannot be deleted is reported", showsAt: map[string]int{"refused": 0}, refuseDel: map[string]bool{"refused": true}, left: []string{"refused"}, waits: 0},
		{name: "a part whose lookup is refused is not asked again and not reported as left", showsAt: map[string]int{"blind": -2}, unchecked: []string{"blind"}, waits: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			round, waits := 0, 0
			partDeleteSleep = func(d time.Duration) {
				mu.Lock()
				defer mu.Unlock()
				if d == partSettlePoll {
					waits++
					round++
				}
			}
			defer func() { partDeleteSleep = time.Sleep }()

			var unknown []*storage.ObjectHandle
			for _, name := range []string{"there", "late", "never", "refused", "blind"} {
				if _, ok := tt.showsAt[name]; ok {
					unknown = append(unknown, obj(name))
				}
			}
			var deleted []string
			left, unchecked := sweepLateParts(unknown, func(h *storage.ObjectHandle) (*storage.ObjectHandle, error) {
				mu.Lock()
				defer mu.Unlock()
				at := tt.showsAt[h.ObjectName()]
				if at == -2 {
					return nil, &googleapi.Error{Code: 403}
				}
				if at < 0 || round < at {
					return nil, nil
				}
				return h.Generation(7), nil
			}, func(h *storage.ObjectHandle) error {
				mu.Lock()
				defer mu.Unlock()
				if tt.refuseDel[h.ObjectName()] {
					return &googleapi.Error{Code: 403}
				}
				deleted = append(deleted, h.ObjectName())
				return nil
			})
			assert.Equal(t, tt.left, left)
			assert.Equal(t, tt.unchecked, unchecked)
			assert.Equal(t, tt.deleted, deleted)
			assert.Equal(t, tt.waits, waits)
		})
	}
}

func TestSniffContentTypeMatchesTheServiceDetection(t *testing.T) {
	dir := t.TempDir()
	text := filepath.Join(dir, "a.csv")
	assert.NoError(t, os.WriteFile(text, []byte("1,2,30\n3,4,40\n"), 0o644))
	bin := filepath.Join(dir, "a.bin")
	assert.NoError(t, os.WriteFile(bin, []byte{0x00, 0x01, 0xff, 0x00, 0x7f}, 0o644))
	ft, _ := os.Open(text)
	defer ft.Close()
	fb, _ := os.Open(bin)
	defer fb.Close()
	assert.Equal(t, "text/plain; charset=utf-8", sniffContentType(ft))
	assert.Equal(t, "application/octet-stream", sniffContentType(fb))
	// the upload reads the file by offset, so sniffing must not move it
	pos, _ := ft.Seek(0, io.SeekCurrent)
	assert.Equal(t, int64(0), pos)
}

func gentleAttrs(content []byte) *storage.ObjectAttrs {
	return &storage.ObjectAttrs{Size: int64(len(content)), CRC32C: crc32.Checksum(content, common.Castagnoli)}
}

func gentleSums(content []byte, cuts ...int) ([]uint32, []int64) {
	var sums []uint32
	var lens []int64
	start := 0
	for _, end := range append(cuts, len(content)) {
		sums = append(sums, crc32.Checksum(content[start:end], common.Castagnoli))
		lens = append(lens, int64(end-start))
		start = end
	}
	return sums, lens
}

func TestVerifyGentleDownloadAcceptsChunkSumsThatAddUpToTheObject(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789abcdef"), 1<<16)
	path := filepath.Join(t.TempDir(), "input.osrm.geometry")
	require.NoError(t, os.WriteFile(path, content, 0644))

	for _, cuts := range [][]int{nil, {1}, {1 << 19, 3 << 18}} {
		sums, lens := gentleSums(content, cuts...)
		assert.NoError(t, verifyGentleDownload(true, path, "b", "o", gentleAttrs(content), sums, lens), "cuts %v", cuts)
	}

	// the sum is now served from the cache: the content below no longer adds up to it
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte("x"), len(content)), 0644))
	require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime()))
	assert.Equal(t, gentleAttrs(content).CRC32C, common.GetFileCRC32C(path))
}

func TestVerifyGentleDownloadRejectsAWrongSumOrAShortFile(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789abcdef"), 1<<12)
	path := filepath.Join(t.TempDir(), "input.osrm.geometry")
	require.NoError(t, os.WriteFile(path, content, 0644))
	attrs := gentleAttrs(content)

	sums, lens := gentleSums(content, 1000)
	sums[1] ^= 1
	assert.Error(t, verifyGentleDownload(true, path, "b", "o", attrs, sums, lens))
	assert.NoError(t, verifyGentleDownload(false, path, "b", "o", attrs, sums, lens), "without -v a mismatch is reported, not fatal")

	sums, lens = gentleSums(content[:len(content)-1], 1000)
	assert.Error(t, verifyGentleDownload(true, path, "b", "o", attrs, sums, lens))
}

func TestVerifyGentleDownloadOfAnEmptyObject(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty")
	require.NoError(t, os.WriteFile(path, nil, 0644))
	assert.NoError(t, verifyGentleDownload(true, path, "b", "o", gentleAttrs(nil), []uint32{0}, []int64{0}))
}
