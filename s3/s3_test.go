package s3

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
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
		{"code NoSuchKey", &smithy.GenericAPIError{Code: "NoSuchKey"}, true},
		{"code NotFound", &smithy.GenericAPIError{Code: "NotFound"}, true},
		// A missing bucket is a 404 as well, so matching on status alone would
		// report a bucket that is not there as an object that is not there.
		{"code NoSuchBucket", &smithy.GenericAPIError{Code: "NoSuchBucket"}, false},
		{"code AccessDenied", &smithy.GenericAPIError{Code: "AccessDenied"}, false},
		{"code SlowDown", &smithy.GenericAPIError{Code: "SlowDown"}, false},
		{"code PermanentRedirect", &smithy.GenericAPIError{Code: "PermanentRedirect"}, false},
		{"a plain error", errors.New("connection reset"), false},
		{"wrapped NoSuchKey", fmt.Errorf("listing: %w", &types.NoSuchKey{}), true},
	} {
		if got := isNotFound(c.err); got != c.want {
			t.Errorf("isNotFound(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestRegionHint(t *testing.T) {
	for _, tc := range []struct {
		name, region, defaultRegion, want string
	}{
		{"no region set", "", "", defaultRegionHint},
		{"AWS_REGION", "us-gov-west-1", "", "us-gov-west-1"},
		{"AWS_DEFAULT_REGION", "", "us-east-1", "us-east-1"},
		{"AWS_REGION wins", "us-gov-east-1", "us-east-1", "us-gov-east-1"},
		{"blank is unset", "  ", "", defaultRegionHint},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AWS_REGION", tc.region)
			t.Setenv("AWS_DEFAULT_REGION", tc.defaultRegion)
			if got := regionHint(); got != tc.want {
				t.Errorf("regionHint() = %q, want %q", got, tc.want)
			}
		})
	}
}

// The line a failed cat prints is the one it printed before cat streamed.
func TestCatFailureKeepsItsText(t *testing.T) {
	assert.Equal(t, "output object failed when create reader with boom", catFailure(errors.New("boom")))
}
