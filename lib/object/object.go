package object

import (
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/nextbillion-ai/gsg/gcs"
	"github.com/nextbillion-ai/gsg/s3"
	"github.com/nextbillion-ai/gsg/system"
)

var urlRe = regexp.MustCompile(`(s3|gs|S3|GS)://([^/]+)(/.*)?`)
var _gs = &gcs.GCS{}
var _s3 = &s3.S3{}

type Object struct {
	Url     string
	ModTime time.Time
}

func parseUrl(url string) (system, bucket, prefix string, err error) {
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

func Read(url string, to io.Writer) error {
	system, bucket, prefix, err := parseUrl(url)
	if err != nil {
		return err
	}
	var rc io.ReadCloser
	switch system {
	case "s3":
		if rc, err = _s3.GetObjectReader(bucket, prefix); err != nil {
			return err
		}
	case "gs":
		if rc, err = _gs.GetObjectReader(bucket, prefix); err != nil {
			return err
		}
	default:
		panic("this is not supposed to happen")
	}
	defer rc.Close()
	if _, err = io.Copy(to, rc); err != nil {
		return err
	}
	return nil
}

func Write(url string, from io.Reader) error {
	system, bucket, prefix, err := parseUrl(url)
	if err != nil {
		return err
	}
	switch system {
	case "s3":
		return _s3.PutObject(bucket, prefix, from)

	case "gs":
		var w io.WriteCloser
		if w, err = _gs.GetObjectWriter(bucket, prefix); err != nil {
			return err
		}
		if _, err = io.Copy(w, from); err != nil {
			_ = w.Close()
			return err
		}
		return w.Close()
	default:
		panic("this is not supposed to happen")
	}
}

func Delete(url string) error {
	system, bucket, prefix, err := parseUrl(url)
	if err != nil {
		return err
	}
	switch system {
	case "s3":
		return _s3.DeleteObject(bucket, prefix)
	case "gs":
		return _gs.DeleteObject(bucket, prefix)
	default:
		panic("this is not supposed to happen")
	}
}

func List(url string, recursive bool) ([]*Object, error) {
	sys, bucket, prefix, err := parseUrl(url)
	if err != nil {
		return nil, err
	}
	var results []*Object
	var fs []*system.FileObject
	switch sys {
	case "s3":
		if fs, err = _s3.List(bucket, prefix, recursive); err != nil {
			return nil, err
		}
	case "gs":
		if fs, err = _gs.List(bucket, prefix, recursive); err != nil {
			return nil, err
		}
	default:
		panic("this is not supposed to happen")
	}
	for _, f := range fs {
		if f.FileType() == system.FileType_Object {
			o := &Object{
				Url: fmt.Sprintf("%s://%s/%s", f.System.Scheme(), f.Bucket, f.Prefix),
			}
			if f.Attributes != nil {
				o.ModTime = f.Attributes.ModTime
			}
			results = append(results, o)
		}
	}
	return results, nil
}
