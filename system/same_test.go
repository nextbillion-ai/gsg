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

var objectTime = time.Date(2026, 11, 5, 2, 14, 22, 0, time.UTC)

// noChecksum is an object as "aws s3 cp" leaves it: S3 stores a CRC64NVME, so
// there is no CRC32C to fetch. calls counts the checksum lookups it costs.
func noChecksum(calls *int) *Attrs {
	return &Attrs{RelativePath: "valhalla_tiles.tar", Size: 100, ModTime: objectTime,
		CalcCRC32C: func() (uint32, bool) { return 0, false },
		NoCRC32C:   func() bool { *calls++; return true }}
}

func localFile(modTime time.Time, crc uint32) *Attrs {
	return &Attrs{RelativePath: "valhalla_tiles.tar", Size: 100, ModTime: modTime,
		CalcCRC32C: func() (uint32, bool) { return crc, true }}
}

// The repeat download: a file an earlier run fetched carries the object's time.
func TestDownloadOfAnObjectWithNoChecksumMatchesOnSizeAndTime(t *testing.T) {
	for _, force := range []bool{true, false} {
		calls := 0
		same, byTime := noChecksum(&calls).SameForDownload(localFile(objectTime, 1), force)
		assert.True(t, same, "force=%v", force)
		assert.True(t, byTime, "force=%v", force)
	}
}

// An object replaced in the cloud has a new time, even at the same size. So
// does a local file changed since. Both must download.
func TestDownloadOfAnObjectWithNoChecksumSeesADifferentTime(t *testing.T) {
	for _, force := range []bool{true, false} {
		for _, modTime := range []time.Time{objectTime.Add(time.Second), objectTime.Add(-time.Second)} {
			calls := 0
			same, byTime := noChecksum(&calls).SameForDownload(localFile(modTime, 1), force)
			assert.False(t, same, "force=%v time=%v", force, modTime)
			assert.False(t, byTime, "force=%v time=%v", force, modTime)
		}
	}
}

func TestDownloadOfAnObjectWithNoChecksumSeesADifferentSize(t *testing.T) {
	calls := 0
	local := localFile(objectTime, 1)
	local.Size = 99
	same, _ := noChecksum(&calls).SameForDownload(local, true)
	assert.False(t, same)
	assert.Equal(t, 0, calls, "a size difference settles it without a request")
}

// Without -v a changed time settles it, as in Same, so no request is spent.
func TestDownloadWithoutVerifySpendsNoRequestOnAChangedObject(t *testing.T) {
	calls := 0
	same, _ := noChecksum(&calls).SameForDownload(localFile(objectTime.Add(time.Hour), 1), false)
	assert.False(t, same)
	assert.Equal(t, 0, calls)
}

// A time that is unknown on either side is no evidence that the two agree.
func TestDownloadWithAnUnknownTimeIsADifference(t *testing.T) {
	calls := 0
	same, byTime := noChecksum(&calls).SameForDownload(localFile(time.Time{}, 1), true)
	assert.False(t, same)
	assert.False(t, byTime)

	object := noChecksum(&calls)
	object.ModTime = time.Time{}
	same, byTime = object.SameForDownload(localFile(time.Time{}, 1), true)
	assert.False(t, same, "two unknown times must not agree")
	assert.False(t, byTime)
}

func TestDownloadWithNoLocalFileIsADifference(t *testing.T) {
	calls := 0
	same, byTime := noChecksum(&calls).SameForDownload(nil, true)
	assert.False(t, same)
	assert.False(t, byTime)
}

// A lookup that failed says nothing about the object, so a matching time must
// not skip it.
func TestDownloadAfterAFailedLookupIsADifference(t *testing.T) {
	object := &Attrs{RelativePath: "valhalla_tiles.tar", Size: 100, ModTime: objectTime,
		CalcCRC32C: func() (uint32, bool) { return 0, false },
		NoCRC32C:   func() bool { return false }}
	same, byTime := object.SameForDownload(localFile(objectTime, 1), true)
	assert.False(t, same)
	assert.False(t, byTime)
}

// An object with a checksum keeps the content check: -v follows the checksum
// whatever the time says, in both directions.
func TestDownloadOfAnObjectWithAChecksumStillComparesContent(t *testing.T) {
	object := func() *Attrs {
		return &Attrs{RelativePath: "valhalla_tiles.tar", Size: 100, ModTime: objectTime,
			CalcCRC32C: func() (uint32, bool) { return 7, true },
			NoCRC32C:   func() bool { return false }}
	}
	same, byTime := object().SameForDownload(localFile(objectTime.Add(time.Hour), 7), true)
	assert.True(t, same, "the same content at a different time")
	assert.False(t, byTime)

	same, byTime = object().SameForDownload(localFile(objectTime, 8), true)
	assert.False(t, same, "different content at the same time")
	assert.False(t, byTime)
}

// A backend that does not say (gcs, oci today) gets exactly what Same gives it.
func TestDownloadFromABackendThatDoesNotSayFollowsSame(t *testing.T) {
	object := func() *Attrs {
		return &Attrs{RelativePath: "valhalla_tiles.tar", Size: 100, ModTime: objectTime,
			CalcCRC32C: func() (uint32, bool) { return 0, false }}
	}
	for _, force := range []bool{true, false} {
		same, byTime := object().SameForDownload(localFile(objectTime, 1), force)
		assert.Equal(t, object().Same(localFile(objectTime, 1), force), same, "force=%v", force)
		assert.False(t, same, "force=%v", force)
		assert.False(t, byTime, "force=%v", force)
	}
}

// Same itself does not change: uploads and cloud-to-cloud copies use it, and a
// destination there has a new time, so the time means nothing.
func TestSameStillTreatsAnObjectWithNoChecksumAsADifference(t *testing.T) {
	calls := 0
	assert.False(t, noChecksum(&calls).Same(localFile(objectTime, 1), true))
	assert.False(t, noChecksum(&calls).Same(localFile(objectTime, 1), false))
	assert.Equal(t, 0, calls)
}
