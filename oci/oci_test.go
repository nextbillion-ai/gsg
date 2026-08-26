package oci

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScheme(t *testing.T) {
	assert.Equal(t, "oci", (&OCI{}).Scheme())
}

func TestSplitBucket(t *testing.T) {
	for _, c := range []struct {
		in, name, namespace string
	}{
		{"bucket", "bucket", ""},
		{"bucket@ns", "bucket", "ns"},
		{"", "", ""},
		// A namespace is a single label, so the last @ is the separator. A
		// bucket name cannot contain @, but splitting on the last one means a
		// surprising name degrades predictably instead of silently swapping
		// the two fields.
		{"we@ird@ns", "we@ird", "ns"},
		// Degenerate spellings must not be read as a valid namespace: an empty
		// namespace means "use the tenancy's own", which is the safe default.
		{"bucket@", "bucket", ""},
		{"@ns", "", "ns"},
	} {
		name, ns := splitBucket(c.in)
		assert.Equal(t, c.name, name, "name of %q", c.in)
		assert.Equal(t, c.namespace, ns, "namespace of %q", c.in)
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
