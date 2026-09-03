package oci

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A copy is addressed to the source's bucket and names the destination's.
//
// This is the one part of the region change that a same-region test cannot
// reach. DestinationRegion is mandatory even when the two regions are equal,
// so it was always sent -- but it was sent as the source's, read from the
// single client the backend used to have. Whenever src.region == dst.region
// the wrong version and the right one build identical requests, so only an
// assertion on the request itself distinguishes them.
func TestCopyRequestNamesTheDestinationsRegionNotTheSources(t *testing.T) {
	src := bucketRef{ns: "nsx", name: "from", region: "ap-singapore-1"}
	dst := bucketRef{ns: "nsy", name: "to", region: "us-phoenix-1"}

	r := copyRequest(src, dst, "a/key.txt", "b/other.txt")

	// Asked of the source's bucket: that is the bucket being told to copy.
	assert.Equal(t, "nsx", *r.NamespaceName)
	assert.Equal(t, "from", *r.BucketName)
	assert.Equal(t, "a/key.txt", *r.SourceObjectName)

	// And every destination field is the destination's own.
	assert.Equal(t, "us-phoenix-1", *r.DestinationRegion,
		"the copy must name where the object is going, not where it came from")
	assert.Equal(t, "nsy", *r.DestinationNamespace)
	assert.Equal(t, "to", *r.DestinationBucket)
	assert.Equal(t, "b/other.txt", *r.DestinationObjectName)
}

// The same-region case must still send the region rather than leaving it
// empty: the field is mandatory, and an absent one is rejected by the service.
func TestCopyRequestStillNamesTheRegionWhenBothAreTheSame(t *testing.T) {
	ref := bucketRef{ns: "nsx", name: "b", region: "ap-singapore-1"}

	r := copyRequest(ref, ref, "a.txt", "b.txt")

	if assert.NotNil(t, r.DestinationRegion) {
		assert.Equal(t, "ap-singapore-1", *r.DestinationRegion)
	}
}

// Each field must point at its own value. Building a request from struct
// fields by address is easy to get wrong in a way that compiles and then sends
// the source's namespace as the destination's.
func TestCopyRequestDoesNotCrossItsFields(t *testing.T) {
	src := bucketRef{ns: "srcns", name: "srcbucket", region: "ap-singapore-1"}
	dst := bucketRef{ns: "dstns", name: "dstbucket", region: "us-phoenix-1"}

	r := copyRequest(src, dst, "srckey", "dstkey")

	for _, c := range []struct{ what, got, want string }{
		{"NamespaceName", *r.NamespaceName, "srcns"},
		{"BucketName", *r.BucketName, "srcbucket"},
		{"SourceObjectName", *r.SourceObjectName, "srckey"},
		{"DestinationRegion", *r.DestinationRegion, "us-phoenix-1"},
		{"DestinationNamespace", *r.DestinationNamespace, "dstns"},
		{"DestinationBucket", *r.DestinationBucket, "dstbucket"},
		{"DestinationObjectName", *r.DestinationObjectName, "dstkey"},
	} {
		assert.Equal(t, c.want, c.got, "%s", c.what)
	}
}
