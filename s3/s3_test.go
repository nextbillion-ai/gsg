package s3

import "testing"

func TestValidLockETag(t *testing.T) {
	for _, c := range []struct {
		etag string
		want bool
	}{
		{`"a3f51a033f988bc3c16d343ac53bb25f"`, true}, // what PutObject returns
		{`"abc-2"`, true}, // a multipart etag is still opaque
		{"", false},       // no receipt
		{`""`, false},     // empty quoted
		{"*", false},      // If-Match: * matches ANY object
		{`"*"`, false},    // and quoted, in case a provider unquotes
		{"a3f51a033f988bc3c16d343ac53bb25f", false}, // unquoted: not an entity-tag
		{"\"abc\r\nIf-Match: *\"", false},           // header injection shape
		{`W/"abc"`, false},                          // weak etag: S3 does not emit these
	} {
		if got := validLockETag(c.etag); got != c.want {
			t.Errorf("validLockETag(%q) = %v, want %v", c.etag, got, c.want)
		}
	}
}
