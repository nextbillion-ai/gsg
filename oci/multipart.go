package oci

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"

	"github.com/nextbillion-ai/gsg/bar"
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
)

const (
	// ociMinPartSize is a floor of our own, not a service limit. Measured, OCI
	// accepted 8 MiB parts; this keeps some headroom under whatever the real
	// minimum is without ever mattering at the default part size.
	ociMinPartSize int64 = 16 * 1024 * 1024

	// ociMaxPartSize is the largest a single part may be.
	ociMaxPartSize int64 = 50 * 1024 * 1024 * 1024

	// ociMultipartThreshold is where parts start paying for their extra round
	// trips. Unlike s3 there is no size that forces multipart -- a single
	// PutObject stored a 40 GiB object fine, and so did Oracle's own CLI with
	// --no-multipart -- so this is purely about speed and recoverability.
	ociMultipartThreshold int64 = 128 * 1024 * 1024
)

// uploadMultipart stores srcFile as one object assembled from parts.
//
// Unlike s3 there is no composite-checksum problem to avoid: OCI reports the
// whole-object CRC32C for a multipart object and keeps the composite form in a
// separate header. Measured -- committing three parts returned
// opc-content-crc32c equal to the local whole-file CRC32C, with
// opc-multipart-md5 alongside it in the "...-3" composite form, and a later
// HeadObject still reported the whole-object value.
//
// The service is asked to verify each part against a checksum computed here,
// so a part corrupted in transit is rejected rather than assembled.
func (o *OCI) uploadMultipart(f *os.File, size int64, spec, object string, partSize, parts int64, pb *bar.ProgressBar) error {
	ref, err := o.resolve(spec)
	if err != nil {
		return err
	}
	c, ns, bucket := ref.c, ref.ns, ref.name
	ctx := context.Background()

	wholeCRC, _, err := crc32cOfReader(f)
	if err != nil {
		return err
	}

	create, err := c.CreateMultipartUpload(ctx, objectstorage.CreateMultipartUploadRequest{
		NamespaceName: &ns, BucketName: &bucket,
		// The algorithm is a request header rather than a field of the details
		// body, which is easy to get wrong -- the details struct has no
		// checksum field at all.
		OpcChecksumAlgorithm:         objectstorage.CreateMultipartUploadOpcChecksumAlgorithmCrc32c,
		CreateMultipartUploadDetails: objectstorage.CreateMultipartUploadDetails{Object: &object},
	})
	if err != nil {
		logger.Info(module, "cannot start a multipart upload of oci://%s/%s: %s", bucket, object, err)
		return err
	}
	uploadID := create.UploadId

	// An upload left neither committed nor aborted keeps billing for the parts
	// already stored, so every path out of here past this point has to abort
	// -- including a panic, which is why this is deferred on a flag rather
	// than called at each return.
	committed := false
	abort := func() {
		if _, aerr := c.AbortMultipartUpload(ctx, objectstorage.AbortMultipartUploadRequest{
			NamespaceName: &ns, BucketName: &bucket, ObjectName: &object, UploadId: uploadID,
		}); aerr != nil {
			logger.Info(module, "could not abort the multipart upload of oci://%s/%s; its parts will bill until removed, by a lifecycle rule or by hand: %s", bucket, object, aerr)
		}
	}

	defer func() {
		if !committed {
			abort()
		}
	}()

	commit := make([]objectstorage.CommitMultipartUploadPartDetails, parts)
	errs := make([]error, parts)
	sem := make(chan struct{}, common.PartConcurrency(parts))
	var wg sync.WaitGroup
	tbl := crc32.MakeTable(crc32.Castagnoli)

	for i := int64(0); i < parts; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			// A panic in one part would otherwise take the process down with
			// the upload still open, leaving its parts to bill. Turn it into
			// an error so the deferred abort above runs.
			defer func() {
				if r := recover(); r != nil {
					errs[i] = fmt.Errorf("oci: panic uploading part %d: %v", i+1, r)
				}
			}()
			sem <- struct{}{}
			defer func() { <-sem }()

			num := int(i + 1)
			off := i * partSize
			length := partSize
			if off+length > size {
				length = size - off
			}
			// The part's own checksum, over exactly the bytes about to be
			// sent. A section reader per part means nothing is buffered and
			// each part stays independently seekable, so the SDK can rewind
			// and retry one part without the whole transfer restarting.
			ph := crc32.New(tbl)
			if _, cerr := io.Copy(ph, io.NewSectionReader(f, off, length)); cerr != nil {
				errs[i] = fmt.Errorf("oci: cannot read part %d of %s: %w", num, f.Name(), cerr)
				return
			}
			partCRC := crc32cToBase64(ph.Sum32())

			out, perr := c.UploadPart(ctx, objectstorage.UploadPartRequest{
				NamespaceName: &ns, BucketName: &bucket, ObjectName: &object,
				UploadId: uploadID, UploadPartNum: &num,
				ContentLength:        &length,
				UploadPartBody:       io.NopCloser(io.NewSectionReader(f, off, length)),
				OpcChecksumAlgorithm: objectstorage.UploadPartOpcChecksumAlgorithmCrc32c,
				OpcContentCrc32c:     &partCRC,
			})
			if perr != nil {
				logger.Info(module, "part %d of oci://%s/%s failed: %s", num, bucket, object, perr)
				errs[i] = perr
				return
			}
			if pb != nil {
				// A whole part at a time. Parts finish out of order, so this
				// is progress by completion rather than by bytes on the wire.
				pb.IncrBy(length)
			}
			n := num
			commit[i] = objectstorage.CommitMultipartUploadPartDetails{PartNum: &n, Etag: out.ETag}
		}(i)
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	sort.Slice(commit, func(a, b int) bool { return *commit[a].PartNum < *commit[b].PartNum })

	cm, err := c.CommitMultipartUpload(ctx, objectstorage.CommitMultipartUploadRequest{
		NamespaceName: &ns, BucketName: &bucket, ObjectName: &object, UploadId: uploadID,
		CommitMultipartUploadDetails: objectstorage.CommitMultipartUploadDetails{PartsToCommit: commit},
	})
	if err != nil {
		logger.Info(module, "cannot commit the multipart upload of oci://%s/%s: %s", bucket, object, err)
		return err
	}

	// The commit reports the whole-object CRC32C of what was assembled. The
	// single-PUT path has the service reject a mismatch outright; multipart
	// cannot, because no single request carries the whole body, so the check
	// happens here instead. An object that assembled into something other than
	// what was read locally is a failure, not something to pass over.
	// Commit publishes the object, so unlike the single-PUT path -- where the
	// service refuses a mismatch before anything is stored -- a bad result is
	// visible by the time it can be detected. It has to be removed rather than
	// merely reported, or a failed upload leaves a wrong object where callers
	// will read it.
	//
	// Reachable without any corruption in transit: if the local file changes
	// after the whole-file pass but before the parts are read, every part
	// checksum still validates and only the whole-object one disagrees.
	want := crc32cToBase64(wholeCRC)
	unusable := ""
	switch {
	case cm.OpcContentCrc32c == nil:
		unusable = "the service reported no checksum for the assembled object"
	case *cm.OpcContentCrc32c != want:
		unusable = fmt.Sprintf("it assembled to checksum %s, but %s was sent", *cm.OpcContentCrc32c, want)
	}
	if unusable != "" {
		committed = true // it exists now; aborting the upload is not what is needed
		if _, derr := c.DeleteObject(ctx, objectstorage.DeleteObjectRequest{
			NamespaceName: &ns, BucketName: &bucket, ObjectName: &object,
		}); derr != nil {
			logger.Info(module, "could not remove the bad object at oci://%s/%s: %s", bucket, object, derr)
		}
		return fmt.Errorf("oci: uploaded oci://%s/%s but %s: the stored object did not match the file and has been removed",
			bucket, object, unusable)
	}

	committed = true
	logger.Info(module, "uploaded oci://%s/%s in %d parts of %d bytes", bucket, object, parts, partSize)
	return nil
}
