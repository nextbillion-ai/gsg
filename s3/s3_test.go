package s3

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

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

func TestIsNotFound(t *testing.T) {
	// Only a genuine absence may read as "not an object". Anything else has to
	// reach the caller, or a failed request becomes a missing object.
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"NoSuchKey", &types.NoSuchKey{}, true},
		{"NotFound", &types.NotFound{}, true},
		{"http 404", &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{Response: &smithyhttp.Response{
				Response: &http.Response{StatusCode: http.StatusNotFound}}}}, true},
		{"http 301 region redirect", &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{Response: &smithyhttp.Response{
				Response: &http.Response{StatusCode: http.StatusMovedPermanently}}}}, false},
		{"http 403 denied", &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{Response: &smithyhttp.Response{
				Response: &http.Response{StatusCode: http.StatusForbidden}}}}, false},
		{"http 503 throttled", &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{Response: &smithyhttp.Response{
				Response: &http.Response{StatusCode: http.StatusServiceUnavailable}}}}, false},
		{"a plain error", errors.New("connection reset"), false},
		{"wrapped NoSuchKey", fmt.Errorf("listing: %w", &types.NoSuchKey{}), true},
	} {
		if got := isNotFound(c.err); got != c.want {
			t.Errorf("isNotFound(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}
