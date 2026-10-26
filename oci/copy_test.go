package oci

import (
	"testing"
	"time"

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

// A small object gets the OCI CLI's default wait: --max-wait-seconds is 1200.
func TestCopyMaxWaitIsTheCLIsTwentyMinutesForASmallObject(t *testing.T) {
	assert.Equal(t, 20*time.Minute, copyMaxWait(1279733, ""))
	assert.Equal(t, 20*time.Minute, copyMaxWait(0, ""))
}

// A large one gets its size at 10 MB/s. The 68.7 GB Valhalla tar took up to
// 18.5 minutes to copy on us-phoenix-1; the old fixed 10 minutes failed it and
// the CLI's 20 left almost no margin.
func TestCopyMaxWaitScalesWithTheObjectsSize(t *testing.T) {
	assert.Equal(t, 6870*time.Second, copyMaxWait(68_700_000_000, ""))
	assert.Greater(t, copyMaxWait(68_700_000_000, ""), 18*time.Minute+30*time.Second)
	// 24 GB copied in up to 10.8 minutes under load: 40 minutes allowed
	assert.Equal(t, 2400*time.Second, copyMaxWait(24_000_000_000, ""))
}

// GSG_OCI_COPY_MAX_WAIT overrides it either way round, as --max-wait-seconds
// does for the CLI: a Go duration or whole seconds.
func TestCopyMaxWaitOverride(t *testing.T) {
	assert.Equal(t, 90*time.Minute, copyMaxWait(68_700_000_000, "90m"))
	assert.Equal(t, 5400*time.Second, copyMaxWait(1, "5400"))
	assert.Equal(t, 3*time.Minute, copyMaxWait(68_700_000_000, "3m"), "an override can also shorten it")
}

// An override that cannot be read must not turn into "do not wait".
func TestCopyMaxWaitIgnoresAnUnreadableOverride(t *testing.T) {
	for _, bad := range []string{"soon", "0", "-5", "0s", "-1m"} {
		assert.Equal(t, 20*time.Minute, copyMaxWait(1, bad), "%q", bad)
	}
}

// Polling starts fast for small objects and backs off to the CLI's 30 seconds.
func TestNextPollBacksOffToThirtySeconds(t *testing.T) {
	var got []time.Duration
	for d := copyPollFirst; len(got) < 9; d = nextPoll(d) {
		got = append(got, d)
	}
	assert.Equal(t, []time.Duration{
		500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second,
		8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second,
	}, got)
}
