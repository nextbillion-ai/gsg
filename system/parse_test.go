package system

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// fakeSystem is enough of an ISystem to be registered and looked up.
type fakeSystem struct{ scheme string }

func (f *fakeSystem) Init(_ ...string) error                 { return nil }
func (f *fakeSystem) Scheme() string                         { return f.scheme }
func (f *fakeSystem) Attributes(_, _ string) (*Attrs, error) { return nil, nil }
func (f *fakeSystem) BatchAttributes(_, _ string, _ bool) ([]*Attrs, error) {
	return nil, nil
}
func (f *fakeSystem) List(_, _ string, _ bool) ([]*FileObject, error) { return nil, nil }
func (f *fakeSystem) DiskUsage(_, _ string, _ bool) ([]DiskUsage, error) {
	return nil, nil
}
func (f *fakeSystem) Delete(_, _ string) error                            { return nil }
func (f *fakeSystem) Copy(_, _, _, _ string) error                        { return nil }
func (f *fakeSystem) Move(_, _, _, _ string) error                        { return nil }
func (f *fakeSystem) Cat(_, _ string) ([]byte, error)                     { return nil, nil }
func (f *fakeSystem) IsObject(_, _ string) (bool, error)                  { return false, nil }
func (f *fakeSystem) IsDirectory(_, _ string) (bool, error)               { return false, nil }
func (f *fakeSystem) Download(_, _, _ string, _ bool, _ RunContext) error { return nil }
func (f *fakeSystem) Upload(_, _, _ string, _ RunContext) error           { return nil }

// An authority spelled user@host must reach the backend intact. OCI uses it to
// carry an explicit namespace; the parser keeps the two joined so that Bucket
// stays one opaque string and only the backend concerned has to interpret it.
func TestParseFileObjectKeepsAnExplicitAuthority(t *testing.T) {
	Register(&fakeSystem{scheme: "oci"})
	Register(&fakeSystem{scheme: "s3"})

	for _, c := range []struct {
		path, bucket, prefix string
	}{
		{"oci://bucket/a/b.txt", "bucket", "a/b.txt"},
		{"oci://bucket@ns/a/b.txt", "bucket@ns", "a/b.txt"},
		{"oci://bucket@ns/", "bucket@ns", ""},
		{"oci://bucket@ns", "bucket@ns", ""},
		// The other backends never use this form and must be untouched.
		{"s3://bucket/a/b.txt", "bucket", "a/b.txt"},
	} {
		fo := ParseFileObject(c.path)
		if assert.NotNil(t, fo, "parsing %q", c.path) {
			assert.Equal(t, c.bucket, fo.Bucket, "bucket of %q", c.path)
			assert.Equal(t, c.prefix, fo.Prefix, "prefix of %q", c.path)
			assert.True(t, fo.Remote, "%q is remote", c.path)
		}
	}
}

// The path a user typed must be the path gsg prints back. Anything that only
// survives one direction shows up here.
func TestExplicitAuthorityRoundTrips(t *testing.T) {
	Register(&fakeSystem{scheme: "oci"})
	for _, p := range []string{
		"oci://bucket/a/b.txt",
		"oci://bucket@ns/a/b.txt",
	} {
		assert.Equal(t, p, ParseFileObject(p).GetFullPath(), "round trip of %q", p)
	}
}
