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

var ErrObjectNotFound = fmt.Errorf("Object Not Found")
var ErrObjectURLInvalid = fmt.Errorf("Object URL Not supported")

type ObjectResult struct {
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

type Object struct {
	_system system.ISystem
	system  string
	bucket  string
	prefix  string
}

func New(url string) (*Object, error) {
	var err error
	var system, bucket, prefix string
	if system, bucket, prefix, err = parseUrl(url); err != nil {
		return nil, err
	}
	obj := &Object{
		system: system,
		bucket: bucket,
		prefix: prefix,
	}
	switch system {
	case "s3":
		obj._system = &s3.S3{}
	case "gs":
		obj._system = &gcs.GCS{}
	default:
		return nil, ErrObjectURLInvalid
	}
	if err = obj._system.Init(bucket); err != nil {
		return nil, err
	}
	return obj, nil
}

func (o *Object) Reset(url string) error {
	var err error
	var system, bucket, prefix string
	if system, bucket, prefix, err = parseUrl(url); err != nil {
		return err
	}
	o.bucket = bucket
	o.prefix = prefix
	if o.system == system {
		if system == "s3" && o.bucket != bucket {
			if err = o._system.Init(o.bucket); err != nil {
				return err
			}
		}
	} else {
		o.system = system
		if err = o._system.Init(o.bucket); err != nil {
			return err
		}
	}
	return nil
}

func (o *Object) Read(to io.Writer) error {
	var err error
	var rc io.ReadCloser
	switch o.system {
	case "s3":
		if rc, err = o._system.(*s3.S3).GetObjectReader(o.bucket, o.prefix); err != nil {
			return ErrObjectNotFound
		}
	case "gs":
		if rc, err = o._system.(*gcs.GCS).GetObjectReader(o.bucket, o.prefix); err != nil {
			return ErrObjectNotFound
		}
	}
	defer rc.Close()
	if _, err = io.Copy(to, rc); err != nil {
		return err
	}
	return nil
}

func (o *Object) Write(from io.Reader) error {
	var err error
	switch o.system {
	case "s3":
		return o._system.(*s3.S3).PutObject(o.bucket, o.prefix, from)
	case "gs":
		var w io.WriteCloser
		if w, err = o._system.(*gcs.GCS).GetObjectWriter(o.bucket, o.prefix); err != nil {
			return err
		}
		if _, err = io.Copy(w, from); err != nil {
			_ = w.Close()
			return err
		}
		return w.Close()
	}
	return nil
}

func (o *Object) Delete() error {
	switch o.system {
	case "s3":
		return o._system.(*s3.S3).DeleteObject(o.bucket, o.prefix)
	case "gs":
		return o._system.(*gcs.GCS).DeleteObject(o.bucket, o.prefix)
	}
	return nil
}

func (o *Object) List(recursive bool) ([]*ObjectResult, error) {
	var err error
	var results []*ObjectResult
	var fs []*system.FileObject
	if fs, err = o._system.List(o.bucket, o.prefix, recursive); err != nil {
		return nil, ErrObjectNotFound
	}
	for _, f := range fs {
		if f.FileType() == system.FileType_Object {
			o := &ObjectResult{
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
