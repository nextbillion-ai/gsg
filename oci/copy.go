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
	c, ns, srcName, err := o.resolve(srcBucket)
	if err != nil {
		return err
	}
	// The destination may name its own namespace. Its bucket is resolved
	// against the same client: one client serves every bucket in a region.
	dstName, dstNs := splitBucket(dstBucket)
	if dstNs == "" {
		dstNs = ns
	}
	if dstName == "" {
		return fmt.Errorf("oci: no destination bucket given")
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
	// spellings: "bucket" and "bucket@namespace" are the same object, and
	// comparing the raw strings misses it.
	if ns == dstNs && srcName == dstName && srcPrefix == dstPrefix {
		return fmt.Errorf("oci: refusing to copy oci://%s/%s onto itself", srcName, srcPrefix)
	}

	// The source must exist. Without this the work request is accepted and
	// then fails asynchronously, which reports the problem far from its cause.
	head, err := o.headObject(srcBucket, srcPrefix)
	if err != nil {
		return err
	}
	if head == nil {
		return fmt.Errorf("oci: no object at oci://%s/%s", srcName, srcPrefix)
	}

	region := o.region
	if region == "" {
		return fmt.Errorf("oci: cannot copy without a region; none is configured")
	}

	r, err := c.CopyObject(context.Background(), objectstorage.CopyObjectRequest{
		NamespaceName: &ns,
		BucketName:    &srcName,
		CopyObjectDetails: objectstorage.CopyObjectDetails{
			SourceObjectName:      &srcPrefix,
			DestinationRegion:     &region,
			DestinationNamespace:  &dstNs,
			DestinationBucket:     &dstName,
			DestinationObjectName: &dstPrefix,
		},
	})
	if err != nil {
		logger.Info(module, "copy of oci://%s/%s failed: %s", srcName, srcPrefix, err)
		return err
	}
	if r.OpcWorkRequestId == nil {
		return fmt.Errorf("oci: copy of oci://%s/%s was accepted without a work request to track", srcName, srcPrefix)
	}
	if err = o.awaitWorkRequest(c, *r.OpcWorkRequestId); err != nil {
		return err
	}
	logger.Info(module, "Copying from bucket[%s] prefix[%s] to bucket[%s] prefix[%s]",
		srcName, srcPrefix, dstName, dstPrefix)
	return nil
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
// object has two spellings -- "bucket" and "bucket@namespace" -- and a raw
// string comparison sees them as different while the service does not.
func (o *OCI) sameObject(srcBucket, srcPrefix, dstBucket, dstPrefix string) (bool, error) {
	if srcPrefix != dstPrefix {
		return false, nil
	}
	_, srcNs, srcName, err := o.resolve(srcBucket)
	if err != nil {
		return false, err
	}
	_, dstNs, dstName, err := o.resolve(dstBucket)
	if err != nil {
		return false, err
	}
	return srcNs == dstNs && srcName == dstName, nil
}

// Move copies an object and then removes the source.
//
// Two things make this safe, and both are load-bearing.
//
// Copy waits for the object to exist. Deleting the source after an
// asynchronous copy that had only been accepted would lose the data outright.
//
// And the source and destination are compared after resolution. Comparing the
// raw strings meant that "oci://b/k" and "oci://b@namespace/k" -- the same
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
	c, ns, name, err := o.resolve(bucket)
	if err != nil {
		return err
	}
	if _, err = c.DeleteObject(context.Background(), objectstorage.DeleteObjectRequest{
		NamespaceName: &ns, BucketName: &name, ObjectName: &prefix,
	}); err != nil {
		logger.Info(module, "cannot delete oci://%s/%s: %s", name, prefix, err)
		return err
	}
	logger.Info(module, "Removing bucket[%s] prefix[%s]", name, prefix)
	return nil
}
