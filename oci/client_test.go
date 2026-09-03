package oci

import (
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
)

// A bucket that exists is checked once and remembered.
func TestVerifyBucketAsksOnlyOnce(t *testing.T) {
	o := &OCI{}
	calls := 0
	check := func() error { calls++; return nil }
	for i := 0; i < 5; i++ {
		assert.NoError(t, o.verifyBucketWith("ap-singapore-1", "ns", "b", check))
	}
	assert.Equal(t, 1, calls, "the bucket should be checked once, not once per use")
}

// A bucket that definitely does not exist is also remembered: repeating the
// question cannot change the answer, and this is the case the whole cache
// exists to keep off the miss path.
func TestVerifyBucketRemembersADefiniteAbsence(t *testing.T) {
	o := &OCI{}
	calls := 0
	check := func() error { calls++; return fakeServiceError{status: 404, code: "BucketNotFound"} }
	for i := 0; i < 3; i++ {
		err := o.verifyBucketWith("ap-singapore-1", "ns", "b", check)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot reach bucket")
	}
	assert.Equal(t, 1, calls, "a definite absence need only be established once")
}

// A transient failure must not be remembered. Caching it would make one
// unlucky moment permanent for the rest of the run: a retry would get the
// remembered error back without ever asking again.
func TestVerifyBucketDoesNotRememberATransientFailure(t *testing.T) {
	for _, transient := range []error{
		fakeServiceError{status: 429, code: "TooManyRequests"},
		fakeServiceError{status: 500, code: "InternalServerError"},
		fakeServiceError{status: 503, code: "ServiceUnavailable"},
		errors.New("dial tcp: connection refused"),
	} {
		o := &OCI{}
		calls := 0
		// Fails once, then succeeds -- which is what a retry is for.
		check := func() error {
			calls++
			if calls == 1 {
				return transient
			}
			return nil
		}
		assert.Error(t, o.verifyBucketWith("ap-singapore-1", "ns", "b", check), "first attempt fails: %v", transient)
		assert.NoError(t, o.verifyBucketWith("ap-singapore-1", "ns", "b", check), "the retry must be allowed to ask again: %v", transient)
		assert.Equal(t, 2, calls, "the check should have been made twice for %v", transient)
	}
}

// Each bucket is remembered separately, and both the region and the namespace
// are part of the identity: one bucket name can exist in more than one of
// either, and those are different buckets.
func TestVerifyBucketKeysOnRegionNamespaceAndBucket(t *testing.T) {
	o := &OCI{}
	calls := map[string]int{}
	mk := func(key string) func() error {
		return func() error { calls[key]++; return nil }
	}
	assert.NoError(t, o.verifyBucketWith("ap-singapore-1", "ns1", "b", mk("ap-singapore-1/ns1/b")))
	assert.NoError(t, o.verifyBucketWith("ap-singapore-1", "ns2", "b", mk("ap-singapore-1/ns2/b")))
	assert.NoError(t, o.verifyBucketWith("ap-singapore-1", "ns1", "other", mk("ap-singapore-1/ns1/other")))
	assert.NoError(t, o.verifyBucketWith("ap-singapore-1", "ns1", "b", mk("ap-singapore-1/ns1/b")))
	assert.NoError(t, o.verifyBucketWith("us-ashburn-1", "ns1", "b", mk("us-ashburn-1/ns1/b")))
	assert.Equal(t, map[string]int{
		"ap-singapore-1/ns1/b":     1,
		"ap-singapore-1/ns2/b":     1,
		"ap-singapore-1/ns1/other": 1,
		// The same name in a second region is a second bucket, and has to be
		// established on its own. Sharing the entry would let a bucket that
		// exists in one region vouch for a name that does not exist in
		// another -- and, once it 404s there, make that absence permanent for
		// the bucket that does exist.
		"us-ashburn-1/ns1/b": 1,
	}, calls)
}

// gsg runs these from a worker pool, so many goroutines hit an unchecked
// bucket at once. Exactly one of them should reach the service.
func TestVerifyBucketUnderConcurrency(t *testing.T) {
	o := &OCI{}
	var mu sync.Mutex
	calls := 0
	check := func() error { mu.Lock(); calls++; mu.Unlock(); return nil }

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); assert.NoError(t, o.verifyBucketWith("ap-singapore-1", "ns", "b", check)) }()
	}
	wg.Wait()
	assert.Equal(t, 1, calls, "50 workers should produce one bucket check, not 50")
}

// TestHTTPClientHasNoWholeRequestDeadline pins the fix for the upload ceiling.
//
// The SDK's default client carries Timeout: 60s, which Go applies to the whole
// request including the body. Because gsg stores an object in a single
// PutObject, that made the largest uploadable object a function of link speed:
// measured, 3 GiB and 6 GiB both failed at ~61s, and both stored fine once the
// deadline was moved. Restoring any non-zero value here brings that back, and
// it would not show up in the UAT -- reproducing it needs a transfer longer
// than a minute, which is bandwidth-dependent and so a flaky thing to assert
// against a live service. Hence a unit test.
func TestHTTPClientHasNoWholeRequestDeadline(t *testing.T) {
	c := newHTTPClient()
	if c.Timeout != 0 {
		t.Fatalf("http client has a whole-request deadline of %s; a request that "+
			"carries the body takes as long as the body needs, so this caps "+
			"uploads at roughly timeout x bandwidth", c.Timeout)
	}
}

// TestHTTPClientStillBoundsADeadPeer is the other half: dropping the
// whole-request deadline is only safe because the transport still notices a
// peer that stops responding. Without these, a stalled connection would hang
// for ever.
//
// That these values actually bite was measured rather than assumed. Against a
// local listener that accepts a PUT, drains the body and then never answers,
// this client gave up after exactly 1m0s with "net/http: timeout awaiting
// response headers". The test that showed it is not kept here because it
// costs a minute to run and the rest of this package finishes in under a
// second; what is kept is the structural check that the values are still set.
func TestHTTPClientStillBoundsADeadPeer(t *testing.T) {
	tr, ok := newHTTPClient().Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T, not *http.Transport, so its timeouts cannot be checked", newHTTPClient().Transport)
	}
	for _, c := range []struct {
		what string
		got  time.Duration
	}{
		{"TLSHandshakeTimeout", tr.TLSHandshakeTimeout},
		{"IdleConnTimeout", tr.IdleConnTimeout},
		{"ExpectContinueTimeout", tr.ExpectContinueTimeout},
		// Safe despite the long uploads: Go starts this clock only after the
		// request body has been fully written.
		{"ResponseHeaderTimeout", tr.ResponseHeaderTimeout},
	} {
		if c.got <= 0 {
			t.Errorf("%s is %s; with no whole-request deadline this is what "+
				"bounds a peer that stops responding", c.what, c.got)
		}
	}
	if tr.DialContext == nil {
		t.Error("DialContext is nil, so connecting to an unreachable host is unbounded")
	}
}

// One process serves many regions, and each gets its own client. A single
// cached client was what confined a run to the region named in ~/.oci/config;
// keying on the region is what lets one command name buckets in several.
func TestClientsAreBuiltPerRegionAndReused(t *testing.T) {
	if _, err := (&OCI{}).clientFor("ap-singapore-1"); err != nil {
		t.Skipf("no usable OCI credentials on this machine: %s", err)
	}
	o := &OCI{}

	sin, err := o.clientFor("ap-singapore-1")
	assert.NoError(t, err)
	iad, err := o.clientFor("us-ashburn-1")
	assert.NoError(t, err)

	assert.NotSame(t, sin, iad, "two regions must not share one client")
	assert.NotEqual(t, sin.Host, iad.Host, "each client must address its own region's endpoint")
	assert.Contains(t, sin.Host, "ap-singapore-1")
	assert.Contains(t, iad.Host, "us-ashburn-1")

	again, err := o.clientFor("ap-singapore-1")
	assert.NoError(t, err)
	assert.Same(t, sin, again, "a region's client should be built once and reused")
	assert.Len(t, o.clients, 2)
}

// stubConfigProvider stands in for credentials that have already been read.
// Embedding the interface satisfies it without implementing anything: nothing
// here calls its methods, only checks which value comes back.
type stubConfigProvider struct {
	ocicommon.ConfigurationProvider
}

// Credentials are read once however many regions are in play. A request is
// signed with the tenancy's key, and that key says nothing about where the
// request is going -- the region is carried by the endpoint. So re-reading
// ~/.oci/config per region would be pure cost, and on a config whose key is
// passphrase-protected it would be a repeated prompt.
func TestCredentialsAreReadOnceForEveryRegion(t *testing.T) {
	o := &OCI{}
	assert.Nil(t, o.provider, "nothing should be read before the first use")
	assert.NotNil(t, o.configProvider(), "the first use reads the credentials")
	assert.NotNil(t, o.provider, "and remembers them")

	// A provider that has already been read is handed back rather than built
	// again -- which is what makes the second region free.
	stub := &stubConfigProvider{}
	o.provider = stub
	assert.Same(t, stub, o.configProvider(), "an already-read provider must be reused")
}

// sameBucket is what Copy and Move ask before refusing to copy an object onto
// itself, and what Move asks before deleting a source. Both halves matter: it
// has to see through the two spellings of one bucket, and it has to keep two
// same-named buckets in different regions apart. Getting the second wrong
// would make a legitimate cross-region copy look like a self-copy -- and in
// cmd/mv.go, which copies and then deletes unconditionally, that loses data.
func TestSameBucketComparesRegionNamespaceAndName(t *testing.T) {
	ref := func(region, ns, name string) bucketRef {
		return bucketRef{region: region, ns: ns, name: name}
	}
	sin := ref("ap-singapore-1", "nsx", "b")

	assert.True(t, sin.sameBucket(ref("ap-singapore-1", "nsx", "b")),
		"the same bucket, however it was spelled, is the same bucket")
	assert.False(t, sin.sameBucket(ref("us-ashburn-1", "nsx", "b")),
		"one name in two regions is two buckets")
	assert.False(t, sin.sameBucket(ref("ap-singapore-1", "nsy", "b")),
		"one name in two namespaces is two buckets")
	assert.False(t, sin.sameBucket(ref("ap-singapore-1", "nsx", "other")),
		"different names are different buckets")
}

// A reference prints the way a path is written, so a message about a bucket
// says which of the possibly several buckets of that name it means.
func TestBucketRefPrintsAsAPath(t *testing.T) {
	assert.Equal(t, "b@ap-singapore-1", bucketRef{name: "b", region: "ap-singapore-1", ns: "nsx"}.String())
}

// CanonicalBucket is what cmd/mv.go compares instead of the raw path, so that
// one bucket spelled two ways is seen as one bucket. A path that already names
// its namespace needs no lookup, which is what makes it answerable here
// without a service.
func TestCanonicalBucketNeedsNoLookupWhenTheNamespaceIsGiven(t *testing.T) {
	o := &OCI{}
	got, err := o.CanonicalBucket("b@axkm4tp1h2ba.ap-singapore-1")
	if assert.NoError(t, err) {
		assert.Equal(t, "b@axkm4tp1h2ba.ap-singapore-1", got)
	}
	assert.Empty(t, o.clients, "an explicit namespace must not build a client")
	assert.Nil(t, o.provider, "nor read any credentials")
}

// Two different buckets must not canonicalise to one string. This is the half
// that keeps a legitimate cross-region move working: collapsing these would
// make cmd/mv.go refuse it as a self-move.
func TestCanonicalBucketKeepsDifferentBucketsApart(t *testing.T) {
	o := &OCI{}
	specs := []string{
		"b@ns1.ap-singapore-1",
		"b@ns1.us-phoenix-1",
		"b@ns2.ap-singapore-1",
		"other@ns1.ap-singapore-1",
	}
	seen := map[string]string{}
	for _, spec := range specs {
		got, err := o.CanonicalBucket(spec)
		assert.NoError(t, err)
		if prev, clash := seen[got]; clash {
			t.Errorf("%q and %q both canonicalise to %q", prev, spec, got)
		}
		seen[got] = spec
	}
}

// An unusable path is an error rather than a string that happens not to match,
// so that cmd/mv.go falls back to comparing what was written instead of
// silently treating two paths as different because one was malformed.
func TestCanonicalBucketReportsAnUnusablePath(t *testing.T) {
	o := &OCI{}
	for _, spec := range []string{"b", "b@sin", "b@not-a-region"} {
		_, err := o.CanonicalBucket(spec)
		assert.Error(t, err, "%q should not canonicalise", spec)
	}
}
