package lock

import (
	"context"
	"fmt"
	"time"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/gcs"
	"github.com/nextbillion-ai/gsg/logger"
)

var g = &gcs.GCS{}

type Distributed struct {
	bucket     string
	prefix     string
	generation int64
}

func New(bucket, prefix string) *Distributed {
	return &Distributed{
		bucket: bucket,
		prefix: prefix,
	}
}

func NewWithUrl(url string) (*Distributed, error) {
	var scheme, bucket, prefix string
	var err error
	if scheme, bucket, prefix, err = common.ParseObjectUrl(url); err != nil {
		return nil, err
	}
	if scheme != "gs" {
		return nil, fmt.Errorf("only gcs locks are supported")
	}
	return &Distributed{
		bucket: bucket,
		prefix: prefix,
	}, nil
}

func (d *Distributed) Lock(ctx context.Context, ttl time.Duration) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			var err error
			if d.generation, err = g.DoAttemptLock(d.bucket, d.prefix, ttl); err == nil {
				logger.Debug("lock", "locked with gen: %d", d.generation)
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (d *Distributed) Unlock() error {
	if d.generation == 0 {
		return fmt.Errorf("unlock failed with generation value == 0. didn't lock first?")
	}
	return g.DoAttemptUnlock(d.bucket, d.prefix, d.generation)
}
