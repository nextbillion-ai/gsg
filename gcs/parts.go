package gcs

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/nextbillion-ai/gsg/logger"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// DefaultStalePartAge is how long the newest part of a composite upload has to
// have stood before SweepParts takes the upload for dead.
//
// A running upload finalizes a part at least once per part's duration: parts
// are at most size/32 and upload in parallel, so a 1 TB file's 32 GB parts would
// have to move below 9 MB/s each for an hour to pass with none finishing, and
// the stall guard fails a part that stops long before that. The compose and the
// part deletes that follow the last part take seconds.
const DefaultStalePartAge = time.Hour

// partName matches the parts of a composite upload: gsg's
// <object>.gsg-part-<upload id>-<NN> and jam-core's <object>.jam-part-<upload id>-<NN>.
var partName = regexp.MustCompile(`^(.+)\.(gsg|jam)-part-([0-9a-f]+)-([0-9]+)$`)

// how long the listing of the automatic sweep before a composite upload may take
var stalePartListTimeout = time.Minute

// a seam for the tests
var sweepNow = time.Now

// PartSweep is what SweepParts did. Names carry the generation, as
// name#generation.
type PartSweep struct {
	// Removed are the parts of dead uploads that were deleted.
	Removed []string
	// Live are parts left alone because another part of the same upload is
	// younger than the age asked for: that upload may still be running.
	Live []string
	// Left are parts of dead uploads that could not be deleted.
	Left []string
}

type partUpload struct {
	newest time.Time
	parts  []*storage.ObjectAttrs
}

// SweepParts deletes the parts that composite uploads under prefix left behind
// when their process died: killed mid-upload, or between the compose and the
// deletes after it. The upload that wrote them has gone, and nothing else knows
// their names, which are random per upload.
//
// An upload is taken for dead only when its newest part, of every part listed
// under the same upload id, is older than olderThan; a running upload adds parts
// as they finish, so its parts are never touched while any of them is young.
// Every part is deleted at the generation listed, noncurrent versions included,
// so a delete can never take an object written after the listing.
//
// prefix is a plain object prefix ("dir/", "dir/file"); the parts of one object
// are under "<object>.". olderThan must be positive.
func (g *GCS) SweepParts(ctx context.Context, bucket, prefix string, olderThan time.Duration) (PartSweep, error) {
	return g.sweepParts(ctx, bucket, prefix, "", olderThan)
}

func (g *GCS) sweepParts(ctx context.Context, bucket, prefix, object string, olderThan time.Duration) (PartSweep, error) {
	var res PartSweep
	if olderThan <= 0 {
		return res, fmt.Errorf("gs: sweeping upload parts needs a positive age, got %s", olderThan)
	}
	if err := g.Init(); err != nil {
		return res, err
	}
	uploads := map[string]*partUpload{}
	it := g.client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix, Versions: true, Projection: storage.ProjectionNoACL})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return res, fmt.Errorf("gs: listing upload parts under gs://%s/%s: %w", bucket, prefix, err)
		}
		m := partName.FindStringSubmatch(attrs.Name)
		if m == nil || (object != "" && m[1] != object) {
			continue
		}
		key := m[1] + "\x00" + m[2] + "\x00" + m[3]
		u := uploads[key]
		if u == nil {
			u = &partUpload{}
			uploads[key] = u
		}
		u.parts = append(u.parts, attrs)
		if attrs.Created.After(u.newest) {
			u.newest = attrs.Created
		}
	}
	cutoff := sweepNow().Add(-olderThan)
	bkt := g.client.Bucket(bucket)
	var stale []*storage.ObjectAttrs
	for _, u := range uploads {
		if u.newest.After(cutoff) {
			for _, p := range u.parts {
				res.Live = append(res.Live, versionName(p.Name, p.Generation))
			}
			continue
		}
		stale = append(stale, u.parts...)
	}
	handles := make([]*storage.ObjectHandle, len(stale))
	for i, p := range stale {
		handles[i] = bkt.Object(p.Name).Generation(p.Generation)
	}
	failed := deletePartsReporting(handles, deletePart)
	for i, p := range stale {
		if failed[i] {
			res.Left = append(res.Left, versionName(p.Name, p.Generation))
		} else {
			res.Removed = append(res.Removed, versionName(p.Name, p.Generation))
		}
	}
	sort.Strings(res.Removed)
	sort.Strings(res.Live)
	sort.Strings(res.Left)
	return res, nil
}

// sweepStaleParts is the sweep a composite upload runs for its own object
// first, so a rerun after a kill removes what the killed run left. Best effort:
// a failure is logged and never fails the upload.
func (g *GCS) sweepStaleParts(bucket, object string) {
	ctx, cancel := context.WithTimeout(context.Background(), stalePartListTimeout)
	defer cancel()
	res, err := g.sweepParts(ctx, bucket, object+".", object, DefaultStalePartAge)
	if err != nil {
		logger.Info(module, "could not sweep stale upload parts of gs://%s/%s: %s", bucket, object, err)
		return
	}
	if len(res.Removed) > 0 {
		logger.Info(module, "removed %d stale upload part(s) of gs://%s/%s", len(res.Removed), bucket, object)
	}
	if len(res.Left) > 0 {
		logger.Info(module, "could not delete stale upload part(s) of gs://%s/%s: %s", bucket, object, strings.Join(res.Left, " "))
	}
}

func versionName(name string, generation int64) string {
	return fmt.Sprintf("%s#%d", name, generation)
}
