package oci

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"time"

	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// headObject returns an object's metadata, or (nil, nil) if the object is
// genuinely not there.
//
// The second part is harder than it looks. A HEAD has no response body, so the
// SDK has nothing to parse an error code out of and reports every 404 as
// "BadErrorResponse" -- measured against a real tenancy, a missing object, a
// missing bucket and a missing namespace are indistinguishable:
//
//	missing object    -> HTTP=404 code="BadErrorResponse"
//	missing bucket    -> HTTP=404 code="BadErrorResponse"
//	missing namespace -> HTTP=404 code="BadErrorResponse"
//
// Treating all of those as "no such object" is the bug this backend must not
// repeat: on the s3 side it made a cross-region redirect read as absence, and
// a caller that believes an object is absent deletes or re-uploads it.
//
// What makes a 404 readable here is that resolve has already established the
// bucket and namespace exist, and remembered it. Existence is a property of
// the bucket rather than of each object in it, so it is settled once per
// bucket instead of being re-asked whenever an object turns out to be
// missing -- which over a large listing would be one extra request per absent
// object.
//
// The gap that leaves is a bucket that disappears part-way through a run:
// its objects would then read as absent rather than as an error. Re-asking on
// every 404 is what closes it, and that is the cost this deliberately avoids.
// It is a narrow gap in practice -- OCI will not delete a bucket that still
// holds objects, so "everything under it is absent" is a true answer by the
// time the bucket can go, and losing access mid-run gives 401 or 403, which
// is not a 404 and is reported as the error it is.
func (o *OCI) headObject(bucketSpec, prefix string) (*objectstorage.HeadObjectResponse, error) {
	c, ns, bucket, err := o.resolve(bucketSpec)
	if err != nil {
		return nil, err
	}
	if prefix == "" {
		return nil, nil
	}
	r, err := c.HeadObject(context.Background(), objectstorage.HeadObjectRequest{
		NamespaceName: &ns, BucketName: &bucket, ObjectName: &prefix,
	})
	if err == nil {
		return &r, nil
	}
	if !isNotFound(err) {
		logger.Info(module, "head oci://%s/%s failed: %s", bucket, prefix, err)
		return nil, err
	}
	// A 404 here means the object is not there. It can mean that only because
	// resolve has already established that the bucket and namespace are, and
	// remembered it: the reply itself cannot distinguish the three, since a
	// HEAD carries no body for the SDK to read a code from.
	logger.Debug(module, "no object at oci://%s/%s", bucket, prefix)
	return nil, nil
}

// isNotFound reports whether err is a 404 from the service, as opposed to a
// request that could not be answered at all. It deliberately does not try to
// say *what* was not found: on a HEAD the service gives us nothing to go on,
// which is why the caller confirms against the bucket.
func isNotFound(err error) bool {
	var se ocicommon.ServiceError
	if ok := asServiceError(err, &se); !ok {
		return false
	}
	return se.GetHTTPStatusCode() == 404
}

func asServiceError(err error, out *ocicommon.ServiceError) bool {
	if se, ok := err.(ocicommon.ServiceError); ok {
		*out = se
		return true
	}
	return false
}

// crc32cOf decodes the checksum OCI reports, and says whether there was one.
//
// OCI stores a CRC32C only when the uploader asked for it -- measured: the same
// bytes uploaded with and without opc-checksum-algorithm come back with and
// without the header, while MD5 is always present. When it is there it covers
// the whole object even for a multipart upload, so unlike s3 there is no
// composite-of-parts case to exclude.
//
// The bool matters: a missing checksum is not a checksum of zero, and callers
// must not compare the two. See TODO item 18.
func crc32cOf(b64 *string) (uint32, bool) {
	if b64 == nil || *b64 == "" {
		return 0, false
	}
	raw, err := base64.StdEncoding.DecodeString(*b64)
	if err != nil || len(raw) != 4 {
		logger.Debug(module, "cannot read crc32c %q", *b64)
		return 0, false
	}
	return binary.BigEndian.Uint32(raw), true
}

// modTime normalises a timestamp to whole seconds.
//
// OCI reports an object's modification time at two different precisions
// depending on how it is asked. A listing carries JSON timestamps with
// milliseconds; a HEAD carries the last-modified HTTP header, which is RFC
// 1123 and has only seconds. Download sets the local file's mtime from the
// HEAD, so the same object read two ways differed by a fraction of a second:
//
//	remote (listing) 07:35:02.81 +0000
//	local  (head)    07:35:02.00 +0000
//
// Attrs.Same compares instants, so every object looked modified and rsync
// re-copied the whole tree on every run -- the same end result as the s3
// checksum bug in #47, from an unrelated cause. Truncating both sides to the
// precision the weaker source can carry is what makes them comparable; a
// local filesystem cannot round-trip more than this from an HTTP header
// anyway.
func modTime(t time.Time) time.Time {
	return t.Truncate(time.Second)
}

// crc32cToBase64 is the wire form of a checksum: four big-endian bytes,
// base64 encoded, which is how OCI both reports and accepts one.
func crc32cToBase64(v uint32) string {
	raw := make([]byte, 4)
	binary.BigEndian.PutUint32(raw, v)
	return base64.StdEncoding.EncodeToString(raw)
}

// Attributes returns size, mtime and checksum for one object, or (nil, nil) if
// there is no such object.
func (o *OCI) Attributes(bucket, prefix string) (*system.Attrs, error) {
	r, err := o.headObject(bucket, prefix)
	if err != nil || r == nil {
		return nil, err
	}
	a := &system.Attrs{}
	if r.ContentLength != nil {
		a.Size = *r.ContentLength
	}
	if r.LastModified != nil {
		a.ModTime = modTime(r.LastModified.Time)
	}
	var stored bool
	if a.CRC32, stored = crc32cOf(r.OpcContentCrc32c); !stored {
		// Say so rather than letting a bare 0 stand. `gsg hash` prints this
		// number, and "Hash (CRC32C): 0" reads as a checksum of zero rather
		// than as no checksum at all. Attrs has no way to carry the
		// difference -- TODO item 18 -- so the log is what carries it.
		logger.Info(module, "no CRC32C stored for oci://%s/%s; it was uploaded without one", bucket, prefix)
	}
	return a, nil
}

// BatchAttributes returns attributes for everything under prefix.
//
// The listing already carries size and mtime, so unlike the s3 backend this
// needs no per-object call for them -- see TODO item 9, which is about exactly
// that cost. It does not carry a CRC32C though, only an MD5, so the checksum
// is left to CalcCRC32C: a closure that heads that one object if and when
// something actually compares it. The linux backend uses the same hook so that
// listing a directory does not hash every file in it.
func (o *OCI) BatchAttributes(bucket, prefix string, recursive bool) ([]*system.Attrs, error) {
	summaries, _, err := o.walkObjects(bucket, prefix, recursive)
	if err != nil {
		return nil, err
	}
	res := make([]*system.Attrs, 0, len(summaries))
	for _, s := range summaries {
		res = append(res, o.summaryToAttrs(bucket, s))
	}
	return res, nil
}

// summaryToAttrs converts one listing entry, deferring the checksum.
func (o *OCI) summaryToAttrs(bucketSpec string, s objectstorage.ObjectSummary) *system.Attrs {
	a := &system.Attrs{}
	if s.Size != nil {
		a.Size = *s.Size
	}
	// Prefer the modification time; a freshly written object may report only a
	// creation time, and an mtime of zero would make every comparison that
	// looks at it fail.
	if s.TimeModified != nil {
		a.ModTime = modTime(s.TimeModified.Time)
	} else if s.TimeCreated != nil {
		a.ModTime = modTime(s.TimeCreated.Time)
	}
	if s.Name != nil {
		name := *s.Name
		a.CalcCRC32C = func() uint32 {
			// The hook can only return a number, so every failure here has to
			// come back as 0 -- and Attrs.Same reads 0 as a real checksum.
			// Two objects that both fail this way compare equal on size
			// alone. That is TODO item 18's missing distinction, and nothing
			// in this backend can repair it, so the least it can do is say so
			// rather than let a silent 0 stand for three different facts.
			r, err := o.headObject(bucketSpec, name)
			if err != nil {
				logger.Info(module, "cannot read the checksum of oci://%s/%s, treating it as absent: %s", bucketSpec, name, err)
				return 0
			}
			if r == nil {
				logger.Info(module, "oci://%s/%s vanished while reading its checksum, treating it as absent", bucketSpec, name)
				return 0
			}
			crc, stored := crc32cOf(r.OpcContentCrc32c)
			if !stored {
				logger.Debug(module, "oci://%s/%s has no stored CRC32C", bucketSpec, name)
			}
			return crc
		}
	}
	return a
}
