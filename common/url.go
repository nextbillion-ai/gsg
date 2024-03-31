package common

import (
	"fmt"
	"regexp"
	"strings"
)

var urlRe = regexp.MustCompile(`(s3|gs|S3|GS)://([^/]+)(/.*)?`)

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
