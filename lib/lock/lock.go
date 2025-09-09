package lock

import (
	"context"
	"fmt"
	"time"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/gcs"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/s3"
)

// StorageLocker defines the interface for distributed locking operations
type StorageLocker interface {
	DoAttemptLock(bucket, object string, ttl time.Duration) (string, error)
	DoAttemptUnlock(bucket, object string, lockId string) error
}

// GCSLocker wraps GCS to implement StorageLocker interface
type GCSLocker struct {
	gcs *gcs.GCS
}

func (g *GCSLocker) DoAttemptLock(bucket, object string, ttl time.Duration) (string, error) {
	generation, err := g.gcs.DoAttemptLock(bucket, object, ttl)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d", generation), nil
}

func (g *GCSLocker) DoAttemptUnlock(bucket, object string, lockId string) error {
	var generation int64
	if _, err := fmt.Sscanf(lockId, "%d", &generation); err != nil {
		return fmt.Errorf("invalid generation ID: %s", lockId)
	}
	return g.gcs.DoAttemptUnlock(bucket, object, generation)
}

// S3Locker wraps S3 to implement StorageLocker interface
type S3Locker struct {
	s3 *s3.S3
}

func (s *S3Locker) DoAttemptLock(bucket, object string, ttl time.Duration) (string, error) {
	return s.s3.DoAttemptLock(bucket, object, ttl)
}

func (s *S3Locker) DoAttemptUnlock(bucket, object string, lockId string) error {
	return s.s3.DoAttemptUnlock(bucket, object, lockId)
}

type Distributed struct {
	bucket  string
	prefix  string
	lockId  string
	storage StorageLocker
}

func NewGCS(bucket, prefix string) *Distributed {
	return &Distributed{
		bucket:  bucket,
		prefix:  prefix,
		storage: &GCSLocker{gcs: &gcs.GCS{}},
	}
}

func NewS3(bucket, prefix string) *Distributed {
	return &Distributed{
		bucket:  bucket,
		prefix:  prefix,
		storage: &S3Locker{s3: &s3.S3{}},
	}
}

// New creates a GCS distributed lock for backward compatibility
func New(bucket, prefix string) *Distributed {
	return NewGCS(bucket, prefix)
}

func NewWithUrl(url string) (*Distributed, error) {
	var scheme, bucket, prefix string
	var err error
	if scheme, bucket, prefix, err = common.ParseObjectUrl(url); err != nil {
		return nil, err
	}

	switch scheme {
	case "gs":
		return NewGCS(bucket, prefix), nil
	case "s3":
		return NewS3(bucket, prefix), nil
	default:
		return nil, fmt.Errorf("unsupported scheme: %s (supported: gs, s3)", scheme)
	}
}

func (d *Distributed) Lock(ctx context.Context, ttl time.Duration) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			var err error
			if d.lockId, err = d.storage.DoAttemptLock(d.bucket, d.prefix, ttl); err == nil {
				logger.Debug("lock", "locked with lockId: %s", d.lockId)
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (d *Distributed) Unlock() error {
	if d.lockId == "" {
		return fmt.Errorf("unlock failed with empty lockId. didn't lock first?")
	}
	return d.storage.DoAttemptUnlock(d.bucket, d.prefix, d.lockId)
}
