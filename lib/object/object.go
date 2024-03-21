package object

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nextbillion-ai/gsg/gcs"
	"github.com/nextbillion-ai/gsg/s3"
	"github.com/nextbillion-ai/gsg/system"
	"golang.org/x/time/rate"
)

var urlRe = regexp.MustCompile(`(s3|gs|S3|GS)://([^/]+)(/.*)?`)

var gcsNotFoundRe = regexp.MustCompile(`.*storage: object doesn't exist.*`)
var s3NotFoundRe = regexp.MustCompile(`.*StatusCode: 404.*`)

var ErrObjectNotFound = fmt.Errorf("Object Not Found")
var ErrObjectURLInvalid = fmt.Errorf("Object URL Not supported")

type rateLimiterCache struct {
	sync.RWMutex
	cache map[string]*rate.Limiter
}

func newRateLimiterCache() *rateLimiterCache {
	return &rateLimiterCache{cache: map[string]*rate.Limiter{}}
}

func (r *rateLimiterCache) Get(url string) *rate.Limiter {
	defer r.RUnlock()
	r.RLock()
	return r.cache[url]
}

func (r *rateLimiterCache) Set(url string, l *rate.Limiter) {
	defer r.Unlock()
	r.Lock()
	r.cache[url] = l
}

func (r *rateLimiterCache) GetOrNew(url string) *rate.Limiter {

	var l = r.Get(url)
	if l != nil {
		return l
	}
	l = rate.NewLimiter(1, 1)
	r.Set(url, l)
	return l

}

var _rlCache = newRateLimiterCache()

type systemCache struct {
	sync.RWMutex
	cache map[string]system.ISystem
}

func (s *systemCache) Get(scheme, bucket string) system.ISystem {
	defer s.RUnlock()
	s.RLock()
	switch scheme {
	case "gs":
		return s.cache["gs"]
	case "s3":
		return s.cache["s3://"+bucket]
	default:
		return nil
	}
}

func (s *systemCache) Set(scheme, bucket string, sys system.ISystem) {
	defer s.Unlock()
	s.Lock()
	switch scheme {
	case "gs":
		s.cache["gs"] = sys
	case "s3":
		s.cache["s3://"+bucket] = sys
	default:
	}
}

func (s *systemCache) GetOrNew(scheme, bucket string) (system.ISystem, error) {
	var sys system.ISystem
	switch scheme {
	case "gs":
		sys = s.Get(scheme, bucket)
		if sys != nil {
			return sys, nil
		}
		sys = &gcs.GCS{}
	case "s3":
		sys = s.Get(scheme, bucket)
		if sys != nil {
			return sys, nil
		}
		sys = &s3.S3{}
	default:
		return nil, ErrObjectURLInvalid
	}

	if err := sys.Init(bucket); err != nil {
		return nil, err
	}
	s.Set(scheme, bucket, sys)
	return sys, nil
}

func newSystemCache() *systemCache {
	return &systemCache{
		cache: map[string]system.ISystem{},
	}
}

var _syscache = newSystemCache()

func parseError(err error) error {
	s := err.Error()
	if gcsNotFoundRe.MatchString(s) {
		return ErrObjectNotFound
	}
	if s3NotFoundRe.MatchString(s) {
		return ErrObjectNotFound
	}
	return err
}

type ObjectResult struct {
	Url     string
	ModTime time.Time
}

func parseUrl(url string) (scheme, bucket, prefix string, err error) {
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
	scheme  string
	bucket  string
	prefix  string
	url     string
}

func New(url string) (*Object, error) {
	var err error
	var scheme, bucket, prefix string
	if scheme, bucket, prefix, err = parseUrl(url); err != nil {
		return nil, err
	}
	var sys system.ISystem
	if sys, err = _syscache.GetOrNew(scheme, bucket); err != nil {
		return nil, err
	}
	return &Object{
		_system: sys,
		scheme:  scheme,
		bucket:  bucket,
		prefix:  prefix,
		url:     url,
	}, nil
}

func (o *Object) Reset(url string) error {
	var err error
	var scheme, bucket, prefix string
	if scheme, bucket, prefix, err = parseUrl(url); err != nil {
		return err
	}
	var sys system.ISystem
	if sys, err = _syscache.GetOrNew(scheme, bucket); err != nil {
		return err
	}
	o.bucket = bucket
	o.prefix = prefix
	o.scheme = scheme
	o.url = url
	o._system = sys
	return nil
}

func (o *Object) Read(to io.Writer) error {
	var err error
	var rc io.ReadCloser
	switch o.scheme {
	case "s3":
		if rc, err = o._system.(*s3.S3).GetObjectReader(o.bucket, o.prefix); err != nil {
			return parseError(err)
		}
	case "gs":
		if rc, err = o._system.(*gcs.GCS).GetObjectReader(o.bucket, o.prefix); err != nil {
			return parseError(err)
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
	if err = _rlCache.GetOrNew(o.url).Wait(context.Background()); err != nil {
		return err
	}
	switch o.scheme {
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
	switch o.scheme {
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
		return nil, parseError(err)
	}
	if len(fs) == 0 {
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
