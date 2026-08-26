package oci

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// lockCachePerm keeps the receipt private. Cross-user unlock cannot work
// anyway: the ETag identifies whoever acquired the lock.
const lockCachePerm = 0600

// lockReceiptPath is where the ETag proving we hold a lock is kept.
//
// The scheme is part of the name, unlike the gs and s3 backends. Theirs hash
// only the bucket and object, so a lock on gs://b/x.lock and one on s3://b/x.lock
// write to the same file and the second overwrites the first's receipt -- TODO
// item 16. Including the scheme here costs nothing because no OCI receipt
// exists yet to be made unreadable, and it keeps this backend out of that
// collision.
func lockReceiptPath(bucket, object string) string {
	return common.GenTempFileName("oci", "://", bucket, "/", object)
}

// validLockETag reports whether a receipt can identify one specific lock.
//
// OCI reports an ETag as a bare UUID rather than the quoted entity-tag s3
// uses, so the check is different, but the reason is the same. Empty is the
// common failure -- a receipt missing, or truncated by a run that died
// mid-write. "*" is the dangerous one: if-match: * matches any object, so a
// receipt corrupted into it would release whichever lock happens to be held,
// which is precisely what the condition exists to prevent.
func validLockETag(etag string) bool {
	if etag == "" || etag == "*" {
		return false
	}
	return !strings.ContainsAny(etag, " \t\r\n\"")
}

// DoAttemptLock takes the lock and returns the ETag that proves it is ours.
func (o *OCI) DoAttemptLock(bucket, object string, ttl time.Duration) (string, error) {
	c, ns, name, err := o.resolve(bucket)
	if err != nil {
		return "", err
	}

	// An existing lock is either still held, in which case we lose, or expired,
	// in which case it is cleared out of the way -- conditionally, so a lock
	// somebody else acquired between the read and the delete survives.
	head, herr := c.HeadObject(context.Background(), objectstorage.HeadObjectRequest{
		NamespaceName: &ns, BucketName: &name, ObjectName: &object,
	})
	if herr == nil {
		if head.LastModified != nil && !head.LastModified.Time.Add(ttl).Before(time.Now()) {
			return "", fmt.Errorf("lock already exists and not expired")
		}
		logger.Debug(module, "DoAttemptLock: clearing an expired lock")
		del := objectstorage.DeleteObjectRequest{
			NamespaceName: &ns, BucketName: &name, ObjectName: &object,
		}
		if head.ETag != nil {
			del.IfMatch = head.ETag
		}
		if _, derr := c.DeleteObject(context.Background(), del); derr != nil {
			// Somebody else cleared or replaced it first. Theirs now.
			logger.Debug(module, "DoAttemptLock: the expired lock changed underneath us: %s", derr)
			return "", fmt.Errorf("lock already exists and not expired")
		}
	}

	// if-none-match: * creates the object only when the key is absent, so
	// exactly one of several contenders wins. Measured against the service:
	// eight concurrent attempts, one winner, the rest refused with
	// IfNoneMatchFailed.
	//
	// No random body is needed to make the ETag distinct, which s3 does need.
	// OCI's ETag is a UUID minted per write rather than a hash of the content:
	// measured, two objects with identical bytes come back with different
	// ETags. The body carries who took it, for anyone looking at the object.
	body := fmt.Sprintf("locked by gsg at %s\n", time.Now().UTC().Format(time.RFC3339))
	size := int64(len(body))
	star := "*"
	put, err := c.PutObject(context.Background(), objectstorage.PutObjectRequest{
		NamespaceName: &ns, BucketName: &name, ObjectName: &object,
		ContentLength: &size, PutObjectBody: io.NopCloser(strings.NewReader(body)),
		IfNoneMatch: &star,
	})
	if err != nil {
		// A refusal here means another contender created it first, which is a
		// lost race rather than a failure of this call.
		logger.Debug(module, "DoAttemptLock: could not create the lock: %s", err)
		return "", fmt.Errorf("lock already exists and not expired")
	}
	if put.ETag == nil || *put.ETag == "" {
		// Without an ETag there is nothing that could later prove the lock is
		// ours, and an unlock would have to be unconditional. Better to fail
		// having taken it than to hold something unreleasable.
		return "", fmt.Errorf("oci: the lock was created but the service returned no ETag")
	}
	return *put.ETag, nil
}

// DoAttemptUnlock releases a lock, but only if this ETag still identifies it.
func (o *OCI) DoAttemptUnlock(bucket, object, etag string) error {
	c, ns, name, err := o.resolve(bucket)
	if err != nil {
		return err
	}
	if !validLockETag(etag) {
		logger.Info(module, "DoAttemptUnlock: unusable ETag %q for oci://%s/%s", etag, name, object)
		return fmt.Errorf("cannot release the lock on oci://%s/%s: %q is not an ETag that proves it is ours", name, object, etag)
	}
	// if-match is what keeps this from releasing somebody else's lock -- one
	// acquired seconds after this caller's own expired, say. Measured: a delete
	// carrying a stale ETag is refused.
	if _, err = c.DeleteObject(context.Background(), objectstorage.DeleteObjectRequest{
		NamespaceName: &ns, BucketName: &name, ObjectName: &object, IfMatch: &etag,
	}); err != nil {
		return err
	}
	return nil
}

// AttemptLock takes the lock and records the receipt that can release it.
func (o *OCI) AttemptLock(bucket, object string, ttl time.Duration) error {
	etag, err := o.DoAttemptLock(bucket, object, ttl)
	if err != nil {
		logger.Info(module, "attempt lock failed: %s", err)
		return err
	}
	logger.Debug(module, "AttemptLock: storing ETag: %s", etag)
	if err = common.WriteFileAtomic(lockReceiptPath(bucket, object), []byte(etag), lockCachePerm); err != nil {
		logger.Info(module, "AttemptLock: cache lock ETag failed: %s", err)
		return err
	}
	return nil
}

// AttemptUnLock releases a lock using the receipt written when it was taken.
//
// A receipt that is simply absent is not an error: there is nothing to
// release, and unlocking something never locked is a no-op. All three existing
// backends draw that line the same way. A receipt that exists but cannot
// identify a lock is different -- the remote lock is real and will now stand
// until its TTL -- so that is reported rather than passed over.
func (o *OCI) AttemptUnLock(bucket, object string) error {
	path := lockReceiptPath(bucket, object)
	raw, err := os.ReadFile(path)
	if err != nil {
		// Said out loud rather than logged at debug. The exit code stays 0 to
		// match gs, s3 and linux, which all treat a missing receipt as nothing
		// to do -- but if the lock object is in fact still there, held by
		// someone else or by an earlier run of ours, "unlocked" is a
		// misleading thing for silence to imply.
		logger.Info(module, "no receipt for oci://%s/%s at %s: nothing was released", bucket, object, path)
		return nil
	}
	etag := strings.TrimSpace(string(raw))
	if err = o.DoAttemptUnlock(bucket, object, etag); err != nil {
		logger.Debug(module, "AttemptUnLock: %s", err)
		return err
	}
	// The receipt has served its purpose and naming a released lock is
	// misleading. Failing to remove it is not fatal: the next acquisition
	// replaces it atomically.
	if rerr := os.Remove(path); rerr != nil {
		logger.Debug(module, "AttemptUnLock: could not remove %s: %s", path, rerr)
	}
	return nil
}
