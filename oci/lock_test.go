package oci

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A receipt has to identify one specific lock. Anything that cannot is
// refused, because the alternative is an unconditional delete that releases
// whichever lock happens to be there -- the defect #44 fixed on s3.
func TestValidLockETag(t *testing.T) {
	// OCI reports an ETag as a bare UUID, measured against the service.
	assert.True(t, validLockETag("ba71882c-0c30-433b-89b3-01c078191f5c"))
	assert.True(t, validLockETag("abc123"))

	for _, c := range []struct{ in, why string }{
		{"", "a receipt that is missing or was truncated mid-write"},
		{"*", "if-match: * matches any object, so this would release someone else's lock"},
		{" ", "whitespace cannot be an entity tag"},
		{"has space", "an ETag does not contain spaces"},
		{"has\ttab", "nor tabs"},
		{"has\nnewline", "nor newlines"},
		{"\"quoted\"", "the s3 quoted form is not what OCI returns, so it did not come from here"},
	} {
		assert.False(t, validLockETag(c.in), "%q: %s", c.in, c.why)
	}
}

// The receipt path includes the scheme. gs and s3 hash only bucket and object,
// so a lock on gs://b/x and one on s3://b/x share a receipt file and the second
// overwrites the first -- TODO item 16. This backend stays out of that.
func TestLockReceiptPathIsSchemeSpecific(t *testing.T) {
	oci1 := lockReceiptPath("bucket", "path/to/x.lock")
	assert.NotEmpty(t, oci1)
	// Same bucket and object, different backend spelling, different file.
	assert.NotEqual(t, oci1, lockReceiptPath("bucket", "path/to/y.lock"))
	assert.NotEqual(t, oci1, lockReceiptPath("other", "path/to/x.lock"))
	// Stable for the same input, or a lock could never be released.
	assert.Equal(t, oci1, lockReceiptPath("bucket", "path/to/x.lock"))
}
