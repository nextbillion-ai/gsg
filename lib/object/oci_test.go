package object

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ErrObjectNotFound is the sentinel consumers check, so every backend's way of
// saying "no such object" has to reach it. Ten programs outside this repo
// import this package; a raw sdk error would match none of their checks.
func TestParseErrorRecognisesEveryBackendsNotFound(t *testing.T) {
	for _, c := range []struct{ label, msg string }{
		{"gs", "storage: object doesn't exist"},
		{"s3", "operation error S3: HeadObject, https response error StatusCode: 404, api error NotFound"},
		{"oci", "Error returned by ObjectStorage Service. Http Status Code: 404. Error Code: ObjectNotFound. Message: The object 'a.txt' was not found"},
	} {
		assert.True(t, errors.Is(parseError(errors.New(c.msg)), ErrObjectNotFound),
			"%s: %q should map to ErrObjectNotFound", c.label, c.msg)
	}
}

// Anything else must survive unchanged. Turning a permission or throttling
// failure into "not found" is how a caller deletes or re-uploads real data.
func TestParseErrorLeavesOtherFailuresAlone(t *testing.T) {
	for _, msg := range []string{
		"Error Code: BucketNotFound. Message: The bucket 'b' does not exist",
		"Error Code: NotAuthenticated",
		"Http Status Code: 429. Error Code: TooManyRequests",
		"Http Status Code: 500",
		"dial tcp: connection refused",
	} {
		err := errors.New(msg)
		assert.False(t, errors.Is(parseError(err), ErrObjectNotFound), "%q must not read as not-found", msg)
		assert.Equal(t, err, parseError(err), "%q should pass through unchanged", msg)
	}
}
