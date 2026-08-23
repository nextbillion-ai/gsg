package oci

import (
	"encoding/base64"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func b64of(v uint32) string {
	raw := make([]byte, 4)
	binary.BigEndian.PutUint32(raw, v)
	return base64.StdEncoding.EncodeToString(raw)
}

func TestCrc32cOf(t *testing.T) {
	// The value measured against a real object: OCI reported "WK0e/g==" and
	// the same bytes decode to 1487740670, which is what `gsg hash` prints.
	real := "WK0e/g=="
	got, ok := crc32cOf(&real)
	assert.True(t, ok)
	assert.Equal(t, uint32(1487740670), got)

	// Round trip across the range, including the two ends, since a
	// sign-extension or endianness slip shows up only at the extremes.
	for _, v := range []uint32{0, 1, 0x7fffffff, 0x80000000, 0xffffffff} {
		e := b64of(v)
		got, ok := crc32cOf(&e)
		assert.True(t, ok, "decoding %d", v)
		assert.Equal(t, v, got, "round trip of %d", v)
	}

	// Absence must be reported as absence, never as a checksum of zero: those
	// are different facts and comparing them is TODO item 18.
	empty := ""
	for _, c := range []struct {
		label string
		in    *string
	}{
		{"nil", nil},
		{"empty", &empty},
	} {
		v, ok := crc32cOf(c.in)
		assert.False(t, ok, "%s must not count as a stored checksum", c.label)
		assert.Equal(t, uint32(0), v)
	}

	// Malformed input must also read as absent rather than as some number.
	for _, bad := range []string{"not base64!!", "AAA=", "", "AAAAAAAA"} {
		b := bad
		v, ok := crc32cOf(&b)
		assert.False(t, ok, "%q must not decode to a checksum", bad)
		assert.Equal(t, uint32(0), v)
	}
}

func TestDedupePrefixes(t *testing.T) {
	// Paging a delimited listing can repeat a common prefix, which would show
	// up as a duplicated row in ls and a double-counted directory in du.
	assert.Equal(t, []string{"a/", "b/", "c/"},
		dedupePrefixes([]string{"a/", "b/", "a/", "c/", "b/", "a/"}))
	// Order is the listing's order, not sorted.
	assert.Equal(t, []string{"z/", "a/"}, dedupePrefixes([]string{"z/", "a/", "z/"}))
	assert.Nil(t, dedupePrefixes(nil))
	assert.Nil(t, dedupePrefixes([]string{}))
}

func TestIsDirectoryMarker(t *testing.T) {
	assert.True(t, isDirectoryMarker("a/"))
	assert.True(t, isDirectoryMarker("a/b/"))
	assert.False(t, isDirectoryMarker("a"))
	assert.False(t, isDirectoryMarker("a/b.txt"))
	assert.False(t, isDirectoryMarker(""))
}
