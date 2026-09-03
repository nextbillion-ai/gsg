package oci

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nextbillion-ai/gsg/logger"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// copyPollInterval and copyTimeout bound the wait for a copy to finish.
//
// Measured against the service, a small object took about 4.4 seconds between
// the request being accepted and the object existing. The timeout is far above
// that so a slow copy is not mistaken for a broken one, and the interval is
// long enough not to hammer the work-request endpoint for every object in a
// tree.
const (
	copyPollInterval = 500 * time.Millisecond
	copyTimeout      = 10 * time.Minute
)

// Copy duplicates an object, and does not return until it exists.
//
// OCI's CopyObject is asynchronous: it returns a work request id in about
// 90ms, and the object appears some seconds later. Returning at that point
// would report success for an object nobody can read yet -- and Move is Copy
// followed by Delete, so it would remove the source before the copy existed.
// That is how data gets lost, so this waits for the work request to complete.
func (o *OCI) Copy(srcBucket, srcPrefix, dstBucket, dstPrefix string) error {
	src, err := o.resolve(srcBucket)
	if err != nil {
		return err
	}
	// The destination is resolved in full, not just parsed. It may name its
	// own namespace and its own region, and resolving establishes that its
	// bucket exists before an asynchronous copy is started against it -- the
	// same reason the source is checked below. The check is cached per bucket,
	// so a tree copy pays for it once.
	dst, err := o.resolve(dstBucket)
	if err != nil {
		return err
	}

	// A copy onto itself is refused, not quietly skipped.
	//
	// This is what protects `gsg mv`. That command does not call Move: it
	// calls doCopy and then deletes the source itself (cmd/mv.go). So a Copy
	// that returned nil here would be followed by an unconditional delete, and
	// the object would be gone -- measured, with the object simply absent
	// afterwards. Returning an error makes the command exit before it deletes
	// anything.
	//
	// The comparison is made after resolution because one object has two
	// spellings: "bucket@region" and "bucket@namespace.region" are the same
	// object, and comparing the raw strings misses it. The region is part of
	// the comparison for the opposite reason: the same bucket name in two
	// regions is two different buckets, and refusing that copy would refuse a
	// legitimate cross-region one.
	if src.sameBucket(dst) && srcPrefix == dstPrefix {
		return fmt.Errorf("oci: refusing to copy oci://%s/%s onto itself", src, srcPrefix)
	}

	// The source must exist. Without this the work request is accepted and
	// then fails asynchronously, which reports the problem far from its cause.
	head, err := o.headObject(srcBucket, srcPrefix)
	if err != nil {
		return err
	}
	if head == nil {
		return fmt.Errorf("oci: no object at oci://%s/%s", src, srcPrefix)
	}

	r, err := src.c.CopyObject(context.Background(), copyRequest(src, dst, srcPrefix, dstPrefix))
	if err != nil {
		logger.Info(module, "copy of oci://%s/%s failed: %s", src, srcPrefix, err)
		return err
	}
	if r.OpcWorkRequestId == nil {
		return fmt.Errorf("oci: copy of oci://%s/%s was accepted without a work request to track", src, srcPrefix)
	}
	// The work request lives in the region that accepted it, so it is followed
	// through the source's client even when the object lands elsewhere.
	if err = o.awaitWorkRequest(src.c, *r.OpcWorkRequestId); err != nil {
		return err
	}
	logger.Info(module, "Copying from bucket[%s] prefix[%s] to bucket[%s] prefix[%s]",
		src, srcPrefix, dst, dstPrefix)
	return nil
}

// copyRequest addresses a copy: which bucket it is asked of, and where the
// object is to end up.
//
// It is a function of its own so that the addressing can be checked without a
// service. The field that matters is DestinationRegion. It is mandatory even
// when the source and destination are in one region, so it was always sent --
// but it used to be sent as the *source's* region, read from the one client
// the backend had. That is invisible for as long as every bucket is in one
// region, and there is no way to see it in a same-region test either: with
// src.region == dst.region the wrong version and this one build byte-identical
// requests. What it did was make a copy to another region quietly target a
// bucket of the same name back in the source region, or fail as absent.
//
// The request itself goes to the source's region, because that is the bucket
// being asked to copy; only the destination is named.
func copyRequest(src, dst bucketRef, srcPrefix, dstPrefix string) objectstorage.CopyObjectRequest {
	return objectstorage.CopyObjectRequest{
		NamespaceName: &src.ns,
		BucketName:    &src.name,
		CopyObjectDetails: objectstorage.CopyObjectDetails{
			SourceObjectName:      &srcPrefix,
			DestinationRegion:     &dst.region,
			DestinationNamespace:  &dst.ns,
			DestinationBucket:     &dst.name,
			DestinationObjectName: &dstPrefix,
		},
	}
}

// awaitWorkRequest blocks until the request finishes, or says why it did not.
func (o *OCI) awaitWorkRequest(c *objectstorage.ObjectStorageClient, id string) error {
	deadline := time.Now().Add(copyTimeout)
	for {
		wr, err := c.GetWorkRequest(context.Background(), objectstorage.GetWorkRequestRequest{
			WorkRequestId: &id,
		})
		if err != nil {
			// The request itself could not be read. Reporting success here
			// would be a guess about work we cannot see.
			logger.Info(module, "cannot read work request %s: %s", id, err)
			return fmt.Errorf("oci: cannot follow the copy: %w", err)
		}
		switch wr.Status {
		case objectstorage.WorkRequestStatusCompleted:
			return nil
		case objectstorage.WorkRequestStatusFailed,
			objectstorage.WorkRequestStatusCanceled:
			return fmt.Errorf("oci: copy %s: %s", strings.ToLower(string(wr.Status)), o.workRequestErrors(c, id))
		}
		// CANCELING is not a result: the cancellation is still in flight, and
		// the errors that explain it may not be recorded yet. Keep polling
		// until it settles into CANCELED, or into something else if the work
		// finished first.
		if time.Now().After(deadline) {
			return fmt.Errorf("oci: copy did not finish within %s (work request %s is %s)",
				copyTimeout, id, wr.Status)
		}
		time.Sleep(copyPollInterval)
	}
}

// workRequestErrors returns what the service said went wrong, for the message.
func (o *OCI) workRequestErrors(c *objectstorage.ObjectStorageClient, id string) string {
	r, err := c.ListWorkRequestErrors(context.Background(), objectstorage.ListWorkRequestErrorsRequest{
		WorkRequestId: &id,
	})
	if err != nil || len(r.Items) == 0 {
		return "the service gave no reason"
	}
	msgs := make([]string, 0, len(r.Items))
	for _, e := range r.Items {
		if e.Message != nil {
			msgs = append(msgs, *e.Message)
		}
	}
	if len(msgs) == 0 {
		return "the service gave no reason"
	}
	return strings.Join(msgs, "; ")
}

// sameObject reports whether two paths name the same stored object.
//
// It must compare what the paths resolve to, not how they are spelled. One
// object has two spellings -- "bucket@region" and "bucket@namespace.region" --
// and a raw string comparison sees them as different while the service does
// not. It must also not treat one name in two regions as one object.
func (o *OCI) sameObject(srcBucket, srcPrefix, dstBucket, dstPrefix string) (bool, error) {
	if srcPrefix != dstPrefix {
		return false, nil
	}
	src, err := o.resolve(srcBucket)
	if err != nil {
		return false, err
	}
	dst, err := o.resolve(dstBucket)
	if err != nil {
		return false, err
	}
	return src.sameBucket(dst), nil
}

// Move copies an object and then removes the source.
//
// Two things make this safe, and both are load-bearing.
//
// Copy waits for the object to exist. Deleting the source after an
// asynchronous copy that had only been accepted would lose the data outright.
//
// And the source and destination are compared after resolution. Comparing the
// raw strings meant that "oci://b@r/k" and "oci://b@namespace.r/k" -- the same
// object, spelled two ways -- looked different here, while Copy resolved them
// as identical and returned without doing anything. Move then deleted the
// source: one object in, nothing out. Measured before the fix, the object was
// simply gone.
func (o *OCI) Move(srcBucket, srcPrefix, dstBucket, dstPrefix string) error {
	same, err := o.sameObject(srcBucket, srcPrefix, dstBucket, dstPrefix)
	if err != nil {
		return err
	}
	if same {
		// Not an error, unlike Copy: asking for an object to end up where it
		// already is has been satisfied. What matters is that it returns
		// before the delete below.
		return nil
	}
	if err := o.Copy(srcBucket, srcPrefix, dstBucket, dstPrefix); err != nil {
		return err
	}
	return o.Delete(srcBucket, srcPrefix)
}

// Delete removes an object.
func (o *OCI) Delete(bucket, prefix string) error {
	ref, err := o.resolve(bucket)
	if err != nil {
		return err
	}
	if _, err = ref.c.DeleteObject(context.Background(), objectstorage.DeleteObjectRequest{
		NamespaceName: &ref.ns, BucketName: &ref.name, ObjectName: &prefix,
	}); err != nil {
		logger.Info(module, "cannot delete oci://%s/%s: %s", ref, prefix, err)
		return err
	}
	logger.Info(module, "Removing bucket[%s] prefix[%s]", ref, prefix)
	return nil
}
