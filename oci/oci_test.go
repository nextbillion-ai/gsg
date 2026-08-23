package oci

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nextbillion-ai/gsg/system"
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

// Every method must return an error rather than panic or silently succeed
// while the backend is a skeleton. A stub that returned nil would read as
// success to callers that only check the error.
func TestUnimplementedMethodsReportRatherThanPretend(t *testing.T) {
	o := &OCI{}
	type call struct {
		name string
		err  error
	}
	_, e1 := o.Attributes("b", "p")
	_, e2 := o.BatchAttributes("b", "p", true)
	_, e3 := o.List("b", "p", true)
	_, e4 := o.DiskUsage("b", "p", true)
	_, e5 := o.Cat("b", "p")
	_, e6 := o.IsObject("b", "p")
	_, e7 := o.IsDirectory("b", "p")
	for _, c := range []call{
		{"Attributes", e1}, {"BatchAttributes", e2}, {"List", e3},
		{"DiskUsage", e4}, {"Cat", e5}, {"IsObject", e6}, {"IsDirectory", e7},
		{"Delete", o.Delete("b", "p")},
		{"Copy", o.Copy("b", "p", "b2", "p2")},
		{"Move", o.Move("b", "p", "b2", "p2")},
		{"Upload", o.Upload("f", "b", "p", system.RunContext{})},
		{"Download", o.Download("b", "p", "f", false, system.RunContext{})},
	} {
		assert.Error(t, c.err, "%s must report that it is not implemented", c.name)
		assert.True(t, strings.Contains(c.err.Error(), "not implemented"),
			"%s error should say so plainly, got %q", c.name, c.err)
	}
}
