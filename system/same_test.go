package system

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// What a server-side OCI copy looks like next to its source: same size, same
// checksums, a later modification time (measured, us-phoenix-1, 2026-10-26).
func copied() (*Attrs, *Attrs) {
	src := &Attrs{RelativePath: "nbval/valhalla_tiles.tar", Size: 64961689600,
		ModTime: time.Date(2026, 10, 21, 23, 41, 51, 0, time.UTC), ContentMD5: "obj3ehZs02MpxY2VUC90iw==-485"}
	dst := &Attrs{RelativePath: "nbval/valhalla_tiles.tar", Size: 64961689600,
		ModTime: time.Date(2026, 10, 26, 8, 22, 46, 0, time.UTC), ContentMD5: "obj3ehZs02MpxY2VUC90iw==-485"}
	return src, dst
}

// Comparing times, a copy always looks changed: this is the default, and it is
// left as it was.
func TestSameByTimeSeesACopyAsChanged(t *testing.T) {
	src, dst := copied()
	assert.False(t, src.Same(dst, false))
}

// Comparing content, equal MD5s are enough, and the CRC32C is not fetched.
func TestSameByContentTrustsAnEqualMd5WithoutARequest(t *testing.T) {
	src, dst := copied()
	fetched := false
	src.CalcCRC32C = func() (uint32, bool) { fetched = true; return 1, true }
	dst.CalcCRC32C = func() (uint32, bool) { fetched = true; return 1, true }
	assert.True(t, src.Same(dst, true))
	assert.False(t, fetched, "an equal listed MD5 must not cost a HEAD")
}

// A multipart MD5 depends on the part size, so different MD5s fall back to the
// CRC32C rather than declaring the objects different.
func TestSameByContentFallsBackToCrcWhenMd5sDiffer(t *testing.T) {
	src, dst := copied()
	dst.ContentMD5 = "differentPartLayout==-130"
	src.CalcCRC32C = func() (uint32, bool) { return 0xe79dcee8, true }
	dst.CalcCRC32C = func() (uint32, bool) { return 0xe79dcee8, true }
	assert.True(t, src.Same(dst, true))

	dst.CalcCRC32C = func() (uint32, bool) { return 0x12345678, true }
	assert.False(t, src.Same(dst, true))
}

// A checksum that cannot be read is a difference, as before: the object is
// copied again rather than skipped on the strength of two unknowns.
func TestSameByContentTreatsAnUnknownChecksumAsADifference(t *testing.T) {
	src, dst := copied()
	src.ContentMD5, dst.ContentMD5 = "", ""
	src.CalcCRC32C = func() (uint32, bool) { return 0, false }
	dst.CalcCRC32C = func() (uint32, bool) { return 0, false }
	assert.False(t, src.Same(dst, true))
}

// Size still comes first: equal MD5s on different sizes cannot happen, but
// must not be read as a match if they did.
func TestSameByContentChecksSizeFirst(t *testing.T) {
	src, dst := copied()
	dst.Size--
	assert.False(t, src.Same(dst, true))
}

type plain struct{ ISystem }
type resets struct {
	ISystem
	v bool
}

func (r resets) CopyResetsModTime() bool { return r.v }

func TestCompareByContent(t *testing.T) {
	assert.True(t, CompareByContent(resets{v: true}, resets{v: true}, false), "both ends reset the time on copy")
	assert.False(t, CompareByContent(plain{}, plain{}, false), "gcs and s3 keep comparing times")
	assert.False(t, CompareByContent(resets{v: true}, plain{}, false))
	assert.False(t, CompareByContent(resets{v: false}, resets{v: false}, false))
	assert.True(t, CompareByContent(plain{}, plain{}, true), "-v still forces content everywhere")
}
