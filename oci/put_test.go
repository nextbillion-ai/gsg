package oci

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"hash/crc32"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// notSeekable hides the Seek method a bytes.Reader would otherwise offer, so
// the spool path can be exercised on the same bytes as the seek path.
type notSeekable struct{ r io.Reader }

func (n notSeekable) Read(p []byte) (int, error) { return n.r.Read(p) }

func wantCRC(b []byte) string {
	raw := make([]byte, 4)
	binary.BigEndian.PutUint32(raw, crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)))
	return base64.StdEncoding.EncodeToString(raw)
}

// measureBody has to produce the same length, checksum and bytes whichever
// route it takes. The upload sends the checksum for the service to verify, so
// a disagreement between the two routes would have the service reject
// perfectly good objects on one of them.
func TestMeasureBodyAgreesOnBothRoutes(t *testing.T) {
	for _, payload := range [][]byte{
		[]byte(""),
		[]byte("short body\n"),
		bytes.Repeat([]byte("x"), 1024*1024),
		{0x00, 0x01, 0xff, 0xfe, 0x00}, // NULs and high bytes survive
	} {
		seekable, n1, sum1, c1, err1 := measureBody(bytes.NewReader(payload))
		if c1 != nil {
			defer c1()
		}
		assert.NoError(t, err1)

		spooled, n2, sum2, c2, err2 := measureBody(notSeekable{bytes.NewReader(payload)})
		if c2 != nil {
			defer c2()
		}
		assert.NoError(t, err2)

		assert.Equal(t, int64(len(payload)), n1, "seekable length")
		assert.Equal(t, int64(len(payload)), n2, "spooled length")
		assert.Equal(t, wantCRC(payload), sum1, "seekable checksum")
		assert.Equal(t, sum1, sum2, "the two routes must agree on the checksum")

		// Both must hand back a reader positioned at the start, or the object
		// would be uploaded truncated -- and the checksum would then not match
		// what was sent.
		got1, _ := io.ReadAll(seekable)
		got2, _ := io.ReadAll(spooled)
		assert.Equal(t, payload, got1, "seekable body")
		assert.Equal(t, payload, got2, "spooled body")
	}
}

// A reader already partway through is measured and rewound to where it was,
// not to the beginning of the underlying data.
func TestMeasureBodyRespectsTheCurrentPosition(t *testing.T) {
	r := strings.NewReader("SKIPMEkeep this")
	_, err := r.Seek(6, io.SeekStart)
	assert.NoError(t, err)

	body, n, sum, cleanup, err := measureBody(r)
	if cleanup != nil {
		defer cleanup()
	}
	assert.NoError(t, err)
	assert.Equal(t, int64(len("keep this")), n)
	assert.Equal(t, wantCRC([]byte("keep this")), sum)
	got, _ := io.ReadAll(body)
	assert.Equal(t, "keep this", string(got))
}

// The spool file must not be left behind.
func TestMeasureBodyCleansUpItsSpool(t *testing.T) {
	_, _, _, cleanup, err := measureBody(notSeekable{strings.NewReader("body")})
	assert.NoError(t, err)
	if assert.NotNil(t, cleanup, "a spooled body needs cleaning up") {
		cleanup()
	}
}
