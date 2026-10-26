package oci

import (
	"testing"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
)

func strp(s string) *string { return &s }

// A HEAD reports content-md5 for a single-part object and opc-multipart-md5
// for a multipart one; the listing's md5 is the same value either way.
func TestMd5OfPrefersContentMd5ThenMultipart(t *testing.T) {
	assert.Equal(t, "RAf3F7w3HDksfz6+SumMCQ==", md5Of(strp("RAf3F7w3HDksfz6+SumMCQ=="), nil))
	assert.Equal(t, "obj3ehZs02MpxY2VUC90iw==-485", md5Of(nil, strp("obj3ehZs02MpxY2VUC90iw==-485")))
	assert.Equal(t, "obj3ehZs02MpxY2VUC90iw==-485", md5Of(strp(""), strp("obj3ehZs02MpxY2VUC90iw==-485")))
	assert.Equal(t, "", md5Of(nil, nil))
}

// The listing's md5 reaches the attributes, so rsync can compare it without a
// request.
func TestSummaryCarriesTheListedMd5(t *testing.T) {
	o := &OCI{}
	a := o.summaryToAttrs("b@ns.r", objectstorage.ObjectSummary{
		Name: strp("valhalla/nbval/valhalla_tiles.tar"), Size: int64p(64961689600),
		Md5: strp("obj3ehZs02MpxY2VUC90iw==-485"),
	})
	assert.Equal(t, "obj3ehZs02MpxY2VUC90iw==-485", a.ContentMD5)
	assert.Equal(t, int64(64961689600), a.Size)
}

func int64p(v int64) *int64 { return &v }

// OCI declares that its copy resets the modification time, which is what
// switches rsync between two OCI locations to comparing content.
func TestOCICopyResetsModTime(t *testing.T) {
	assert.True(t, (&OCI{}).CopyResetsModTime())
}
