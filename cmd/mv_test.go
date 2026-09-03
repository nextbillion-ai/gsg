package cmd

import (
	"errors"
	"strings"
	"testing"

	"github.com/nextbillion-ai/gsg/system"
	"github.com/stretchr/testify/assert"
)

type fakeMvSystem struct {
	system.ISystem
	scheme string
}

func (f *fakeMvSystem) Scheme() string { return f.scheme }

func fo(sys system.ISystem, bucket, prefix string) *system.FileObject {
	return &system.FileObject{System: sys, Bucket: bucket, Prefix: prefix, Remote: true}
}

// The guard has three answers, and the two safe ones are not interchangeable:
// "no" lets the move run, while an error refuses it. These helpers assert the
// answer and that it was reached without the third.
func assertTrueDestroys(t *testing.T, a, b *system.FileObject, msgAndArgs ...any) {
	t.Helper()
	got, err := wouldDestroySource(a, b)
	assert.NoError(t, err, msgAndArgs...)
	assert.True(t, got, msgAndArgs...)
}

func assertFalseDestroys(t *testing.T, a, b *system.FileObject, msgAndArgs ...any) {
	t.Helper()
	got, err := wouldDestroySource(a, b)
	assert.NoError(t, err, msgAndArgs...)
	assert.False(t, got, msgAndArgs...)
}

// mv is a copy followed by a delete of the source, so a destination that is
// the source, or inside it, means the delete removes what the copy wrote.
// gsutil refuses the identical case with "are the same file - abort"; it
// allows the nested one because its copy nests the source directory, which
// gsg's cp -r does not.
func TestWouldDestroySource(t *testing.T) {
	gs := &fakeMvSystem{scheme: "gs"}
	s3 := &fakeMvSystem{scheme: "s3"}

	assertTrueDestroys(t, fo(gs, "b", "a.txt"), fo(gs, "b", "a.txt"))

	// A trailing slash is one keystroke and names the same place, because
	// gsg's cp -r copies a directory's contents rather than the directory.
	assertTrueDestroys(t, fo(gs, "b", "d"), fo(gs, "b", "d/"))
	assertTrueDestroys(t, fo(gs, "b", "d/"), fo(gs, "b", "d"))
	assertTrueDestroys(t, fo(gs, "b", "d/"), fo(gs, "b", "d/"))

	// The destination inside the source. gsg's cp -r flattens, so d/a.txt and
	// d/sub/a.txt would both want to become d/sub/a.txt -- measured, that lost
	// the first one.
	assertTrueDestroys(t, fo(gs, "b", "d"), fo(gs, "b", "d/sub"))
	assertTrueDestroys(t, fo(gs, "b", "d/"), fo(gs, "b", "d/sub/deeper"))

	for _, c := range []struct {
		a, b *system.FileObject
		why  string
	}{
		{fo(gs, "b", "a.txt"), fo(gs, "b", "b.txt"), "different objects"},
		{fo(gs, "b", "a.txt"), fo(gs, "other", "a.txt"), "different buckets"},
		{fo(gs, "b", "a.txt"), fo(s3, "b", "a.txt"), "different backends"},
		// A sibling whose name merely begins with the source's.
		{fo(gs, "b", "d"), fo(gs, "b", "dsub"), "a sibling with a shared prefix"},
		{fo(gs, "b", "a.txt"), fo(gs, "b", "a.txt.bak"), "a longer name"},
		// A path that did not parse names nothing.
		{nil, fo(gs, "b", "a.txt"), "nil source"},
		{fo(gs, "b", "a.txt"), nil, "nil destination"},
		{&system.FileObject{}, fo(gs, "b", "a.txt"), "source with no backend"},
	} {
		assertFalseDestroys(t, c.a, c.b, c.why)
	}
}

// canonicalMvSystem stands in for a backend where one bucket has more than one
// spelling, which is what oci is: "b@region" and "b@ns.region" are the same
// bucket. The rule below is that shape in miniature -- a spec with no "." is
// the same bucket as the same name with the tenancy's namespace written in.
type canonicalMvSystem struct {
	system.ISystem
	scheme string
	err    error
	calls  int
}

func (f *canonicalMvSystem) Scheme() string { return f.scheme }

func (f *canonicalMvSystem) CanonicalBucket(spec string) (string, error) {
	f.calls++
	if f.err != nil {
		return "", f.err
	}
	name, qualifier, found := strings.Cut(spec, "@")
	if !found {
		return spec, nil
	}
	if !strings.Contains(qualifier, ".") {
		qualifier = "tenancyns." + qualifier
	}
	return name + "@" + qualifier, nil
}

// The bug this closes: a recursive move into the source's own descendant, with
// the destination spelled using the explicit namespace, looked like two
// different buckets and passed the guard. The copy then wrote into the tree
// being moved and the delete ran over a source list computed before it.
func TestWouldDestroySourceSeesThroughBucketSpellings(t *testing.T) {
	oci := &canonicalMvSystem{scheme: "oci"}

	assertTrueDestroys(t,
		fo(oci, "b@ap-singapore-1", "d"),
		fo(oci, "b@tenancyns.ap-singapore-1", "d/sub"),
		"one bucket spelled two ways is still one bucket")

	assertTrueDestroys(t,
		fo(oci, "b@tenancyns.ap-singapore-1", "a.txt"),
		fo(oci, "b@ap-singapore-1", "a.txt"),
		"and the identical-object case holds whichever way round it is spelled")

	// The other half: resolving must not collapse buckets that really are
	// different. A same-named bucket in another region is another bucket, and
	// refusing that move would refuse a legitimate cross-region one.
	assertFalseDestroys(t,
		fo(oci, "b@ap-singapore-1", "d"),
		fo(oci, "b@us-phoenix-1", "d/sub"),
		"one name in two regions is two buckets")
	assertFalseDestroys(t,
		fo(oci, "b@ap-singapore-1", "d"),
		fo(oci, "b@othertenancy.ap-singapore-1", "d/sub"),
		"one name in two namespaces is two buckets")
}

// A backend that cannot canonicalise -- no credentials, a namespace lookup
// that failed for a moment -- must refuse rather than fall through.
//
// The first version of this fell back to comparing the paths as written, which
// reproduced the original bug exactly: the two spellings compare unequal, the
// guard says no, and the copy and delete that follow resolve the namespace
// perfectly well on their next attempt. A refused move costs a retry; an
// allowed one costs an object.
func TestWouldDestroySourceRefusesWhenItCannotTell(t *testing.T) {
	oci := &canonicalMvSystem{scheme: "oci", err: errors.New("no credentials")}

	_, err := wouldDestroySource(
		fo(oci, "b@ap-singapore-1", "d"),
		fo(oci, "b@tenancyns.ap-singapore-1", "d/sub"),
	)
	assert.Error(t, err, "an unresolvable pair over colliding prefixes must not be waved through")

	// Identical spellings still need no resolution, so a broken backend does
	// not stop the guard from catching the obvious case.
	assertTrueDestroys(t, fo(oci, "b@ap-singapore-1", "d"), fo(oci, "b@ap-singapore-1", "d/sub"))

	// And prefixes that cannot collide are settled before the buckets are
	// looked at, so an ordinary move is never refused for want of credentials.
	assertFalseDestroys(t, fo(oci, "b@ap-singapore-1", "d"), fo(oci, "b@tenancyns.ap-singapore-1", "elsewhere"))
}

// A backend that gives one bucket one spelling is not asked at all.
func TestWouldDestroySourceDoesNotCanonicaliseBackendsThatNeedNot(t *testing.T) {
	gs := &fakeMvSystem{scheme: "gs"}
	assertTrueDestroys(t, fo(gs, "b", "d"), fo(gs, "b", "d/sub"))
	assertFalseDestroys(t, fo(gs, "b", "d"), fo(gs, "other", "d/sub"))
}

// Two paths spelled the same way are the common case, and must not cost a
// namespace lookup each time they are compared.
// Resolution costs a namespace lookup, so it happens only when it can change
// the answer: never for identical spellings, and never when the prefixes
// cannot collide however the buckets resolve.
func TestCanonicalBucketIsAskedOnlyWhenItCanChangeTheAnswer(t *testing.T) {
	same := &canonicalMvSystem{scheme: "oci"}
	_, _ = wouldDestroySource(fo(same, "b@ap-singapore-1", "d"), fo(same, "b@ap-singapore-1", "d/sub"))
	assert.Equal(t, 0, same.calls, "identical spellings settle it without resolving")

	apart := &canonicalMvSystem{scheme: "oci"}
	_, _ = wouldDestroySource(fo(apart, "b@ap-singapore-1", "d"), fo(apart, "b@tenancyns.ap-singapore-1", "elsewhere"))
	assert.Equal(t, 0, apart.calls, "prefixes that cannot collide settle it without resolving")

	colliding := &canonicalMvSystem{scheme: "oci"}
	_, _ = wouldDestroySource(fo(colliding, "b@ap-singapore-1", "d"), fo(colliding, "b@tenancyns.ap-singapore-1", "d/sub"))
	assert.Equal(t, 2, colliding.calls, "one resolution per path when the answer actually turns on it")
}
