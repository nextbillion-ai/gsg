package common

import (
	"fmt"
	"regexp"
	"strings"
)

// The authority is [^/]+ rather than a bucket name, which matters for oci:
// its paths may carry an explicit namespace as "bucket@namespace", and that
// whole string is what the backend expects to receive as the bucket.
// Anchored at both ends. Unanchored, FindStringSubmatch matched anywhere in
// the string, so "notoci://bucket/key" parsed as a valid oci url and a caller
// would operate on that object having supplied a scheme that does not exist.
// The same held for "nots3://" and "xgs://".
//
// The authority is [^/]+ rather than a bucket name, which matters for oci:
// its paths may carry an explicit namespace as "bucket@namespace", and that
// whole string is what the backend expects to receive as the bucket.
var urlRe = regexp.MustCompile(`^(s3|gs|oci|S3|GS|OCI)://([^/]+)(/.*)?$`)

func ParseObjectUrl(url string) (scheme, bucket, prefix string, err error) {
	match := urlRe.FindStringSubmatch(url)
	if len(match) != 4 {
		err = fmt.Errorf("invalid object url: %s", url)
		return
	}
	if len(match[3]) > 0 {
		match[3] = match[3][1:]
	}
	return strings.ToLower(match[1]), match[2], match[3], nil
}
