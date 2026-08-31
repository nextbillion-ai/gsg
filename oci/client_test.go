package oci

import (
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// A bucket that exists is checked once and remembered.
func TestVerifyBucketAsksOnlyOnce(t *testing.T) {
	o := &OCI{}
	calls := 0
	check := func() error { calls++; return nil }
	for i := 0; i < 5; i++ {
		assert.NoError(t, o.verifyBucketWith("ns", "b", check))
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
		err := o.verifyBucketWith("ns", "b", check)
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
		assert.Error(t, o.verifyBucketWith("ns", "b", check), "first attempt fails: %v", transient)
		assert.NoError(t, o.verifyBucketWith("ns", "b", check), "the retry must be allowed to ask again: %v", transient)
		assert.Equal(t, 2, calls, "the check should have been made twice for %v", transient)
	}
}

// Each bucket is remembered separately, and the namespace is part of the
// identity: one bucket name can exist in more than one namespace.
func TestVerifyBucketKeysOnNamespaceAndBucket(t *testing.T) {
	o := &OCI{}
	calls := map[string]int{}
	mk := func(key string) func() error {
		return func() error { calls[key]++; return nil }
	}
	assert.NoError(t, o.verifyBucketWith("ns1", "b", mk("ns1/b")))
	assert.NoError(t, o.verifyBucketWith("ns2", "b", mk("ns2/b")))
	assert.NoError(t, o.verifyBucketWith("ns1", "other", mk("ns1/other")))
	assert.NoError(t, o.verifyBucketWith("ns1", "b", mk("ns1/b")))
	assert.Equal(t, map[string]int{"ns1/b": 1, "ns2/b": 1, "ns1/other": 1}, calls)
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
		go func() { defer wg.Done(); assert.NoError(t, o.verifyBucketWith("ns", "b", check)) }()
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
