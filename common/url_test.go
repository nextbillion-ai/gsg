package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseObjectUrl(t *testing.T) {
	for _, c := range []struct{ url, scheme, bucket, prefix string }{
		{"gs://bucket/a/b.txt", "gs", "bucket", "a/b.txt"},
		{"s3://bucket/a/b.txt", "s3", "bucket", "a/b.txt"},
		{"oci://bucket/a/b.txt", "oci", "bucket", "a/b.txt"},
		// An OCI path may name its namespace, and the whole authority is what
		// the backend expects as its bucket.
		{"oci://bucket@namespace/a/b.txt", "oci", "bucket@namespace", "a/b.txt"},
		// The scheme is matched case insensitively and reported lowercased.
		{"OCI://bucket/a/b.txt", "oci", "bucket", "a/b.txt"},
		{"S3://bucket/a/b.txt", "s3", "bucket", "a/b.txt"},
		// A bucket with no prefix.
		{"oci://bucket", "oci", "bucket", ""},
	} {
		s, b, p, err := ParseObjectUrl(c.url)
		assert.NoError(t, err, c.url)
		assert.Equal(t, c.scheme, s, "scheme of %s", c.url)
		assert.Equal(t, c.bucket, b, "bucket of %s", c.url)
		assert.Equal(t, c.prefix, p, "prefix of %s", c.url)
	}
}

// The pattern used to match anywhere in the string, so a url whose scheme was
// not a scheme at all still parsed -- and the caller went on to operate on
// that object.
func TestParseObjectUrlRejectsAnythingButAWholeUrl(t *testing.T) {
	for _, c := range []struct{ url, why string }{
		{"notoci://bucket/key", "oci:// appears inside a scheme that is not one"},
		{"nots3://bucket/key", "same for s3"},
		{"xgs://bucket/key", "same for gs"},
		{"  s3://bucket/key", "leading whitespace is not part of a url"},
		{"s3://bucket/key\n", "nor a trailing newline"},
		{"ftp://bucket/key", "an unsupported scheme"},
		{"bucket/key", "no scheme at all"},
		{"", "empty"},
		{"s3://", "no bucket"},
	} {
		_, _, _, err := ParseObjectUrl(c.url)
		assert.Error(t, err, "%q should be rejected: %s", c.url, c.why)
	}
}
