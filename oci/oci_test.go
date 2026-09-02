package oci

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScheme(t *testing.T) {
	assert.Equal(t, "oci", (&OCI{}).Scheme())
}

// The everyday spellings, and what each part of them means.
func TestParseBucketSpecReadsEveryAcceptedForm(t *testing.T) {
	for _, c := range []struct {
		in, name, namespace, region string
	}{
		// One component after "@" is the region: the mandatory part, and the
		// form nearly every command will use.
		{"bucket@ap-singapore-1", "bucket", "", "ap-singapore-1"},
		// Two components add the namespace, for a bucket in another tenancy.
		{"bucket@axkm4tp1h2ba.ap-singapore-1", "bucket", "axkm4tp1h2ba", "ap-singapore-1"},
		// Government realms spell a region with four parts rather than three.
		{"bucket@us-gov-ashburn-1", "bucket", "", "us-gov-ashburn-1"},
		// A bucket name may contain dots and dashes; only what follows the
		// last "@" is qualifier, so they cannot be mistaken for one.
		{"my.bucket-2@us-ashburn-1", "my.bucket-2", "", "us-ashburn-1"},
		// A bucket name cannot contain "@", but splitting on the last one
		// means a surprising name degrades predictably instead of silently
		// swapping the fields.
		{"we@ird@us-ashburn-1", "we@ird", "", "us-ashburn-1"},
		// A namespace may contain dots -- a few older tenancies carry them --
		// so the region is what follows the LAST dot, not the first. Splitting
		// on the first would make those namespaces unaddressable.
		{"bucket@old.ns.ap-singapore-1", "bucket", "old.ns", "ap-singapore-1"},
		{"bucket@has_underscore-and-dash.us-ashburn-1", "bucket", "has_underscore-and-dash", "us-ashburn-1"},
	} {
		got, err := parseBucketSpec(c.in)
		if assert.NoError(t, err, "parsing %q", c.in) {
			assert.Equal(t, c.name, got.name, "name of %q", c.in)
			assert.Equal(t, c.namespace, got.namespace, "namespace of %q", c.in)
			assert.Equal(t, c.region, got.region, "region of %q", c.in)
		}
	}
}

// A path with no region is refused rather than filled in from the ambient
// config. This is the whole point of the change: the region decides which of
// possibly several buckets of one name is meant, so inferring it from the
// machine makes one path mean different things in different places.
func TestParseBucketSpecRefusesAPathWithNoRegion(t *testing.T) {
	_, err := parseBucketSpec("bucket")
	if assert.Error(t, err, "a bare bucket name must not resolve") {
		assert.Contains(t, err.Error(), "no region")
		// The message has to carry the fix, since every existing path hits it.
		assert.Contains(t, err.Error(), "bucket@ap-singapore-1")
	}
}

// Short region names are refused in favour of the full form. Of the 78 short
// codes the SDK knows, 18 pairs are one keystroke apart -- so a slip has a
// real chance of naming a different live region, passing validation, and
// surfacing much later as a missing bucket.
func TestParseBucketSpecRefusesShortRegionNames(t *testing.T) {
	for _, c := range []struct{ short, full string }{
		{"sin", "ap-singapore-1"},
		{"iad", "us-ashburn-1"},
		{"fra", "eu-frankfurt-1"},
	} {
		_, err := parseBucketSpec("bucket@" + c.short)
		if assert.Error(t, err, "%q must not be accepted", c.short) {
			assert.Contains(t, err.Error(), "short region name")
			// Naming the expansion is what makes this self-correcting.
			assert.Contains(t, err.Error(), c.full)
		}
	}
}

// A region has one spelling, so a differently-cased one is refused rather than
// folded. Folding would give one bucket two path spellings, and cmd/mv.go's
// guard against a recursive move into its own descendant compares the bucket
// as written -- so every extra spelling is a way around it.
func TestParseBucketSpecRefusesADifferentlyCasedRegion(t *testing.T) {
	_, err := parseBucketSpec("bucket@AP-Singapore-1")
	if assert.Error(t, err, "case must not be folded away") {
		assert.Contains(t, err.Error(), "lower case")
		assert.Contains(t, err.Error(), "ap-singapore-1")
	}
}

// Everything that is not a usable address is an error, not a default. A
// default here would be a guess about which bucket the user meant.
func TestParseBucketSpecRefusesDegenerateSpellings(t *testing.T) {
	for _, c := range []struct{ in, because string }{
		{"", "an empty spec names nothing"},
		{"bucket", "no region"},
		{"bucket@", "an empty qualifier names no region"},
		{"@ap-singapore-1", "no bucket"},
		{"bucket@.ap-singapore-1", "an empty namespace is not the same as omitting it"},
		{"bucket@ns.", "an empty region"},
		{"bucket@not-a-region", "not a region name"},
		{"bucket@ap-singapore", "a region name ends in a number"},
		{"bucket@apsingapore1", "a region name is hyphenated"},
	} {
		_, err := parseBucketSpec(c.in)
		assert.Error(t, err, "%q must be refused: %s", c.in, c.because)
	}
}

// Every error has to name the path it came from. These are reported far from
// where the path was typed -- a worker deep in a recursive copy -- so an error
// that only says "not a region name" leaves the user hunting.
func TestParseBucketSpecErrorsNameThePath(t *testing.T) {
	for _, in := range []string{"bucket", "bucket@sin", "bucket@not-a-region", "bucket@ns.not-a-region"} {
		_, err := parseBucketSpec(in)
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), in, "the error for %q should quote it", in)
		}
	}
}

// An unrecognised but well-formed region is accepted and left to the service.
//
// The SDK exports no list of regions to check against, and a list hard-coded
// here would go stale every time Oracle opens a region. Rejecting a region
// that genuinely exists is the worse failure of the two: it leaves the user
// with no way to proceed, while a typo of this shape fails against the
// endpoint moments later.
func TestParseBucketSpecAllowsAnUnknownButWellFormedRegion(t *testing.T) {
	got, err := parseBucketSpec("bucket@xx-nowhere-9")
	if assert.NoError(t, err) {
		assert.Equal(t, "xx-nowhere-9", got.region)
	}
}

// errNotImplemented is what every not-yet-built operation returns, so it has
// to report plainly rather than look like success.
//
// This deliberately tests the helper rather than enumerating which operations
// are still stubs. An enumeration would have to be edited by every PR in this
// series -- and since those PRs are open at the same time, they would all
// conflict on this one file, which is exactly what the per-file layout exists
// to avoid. Each operation's own test file proves that operation works.
func TestErrNotImplementedReportsRatherThanPretends(t *testing.T) {
	err := errNotImplemented("SomeOperation")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SomeOperation")
	assert.Contains(t, err.Error(), "not implemented")
}
