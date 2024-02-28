package lock

import (
	"context"
	"fmt"
	"time"

	"github.com/nextbillion-ai/gsg/gcs"
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

func (d *Distributed) Lock(ctx context.Context, ttl time.Duration) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			var err error
			if d.generation, err = g.DoAttemptLock(d.bucket, d.prefix, ttl); err == nil {
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
	g.AttemptUnLock(d.bucket, d.prefix)
	return nil
}
