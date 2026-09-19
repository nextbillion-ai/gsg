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

// sourceMoved reports whether the file changed while its parts were being read.
//
// Size or modification time, because the alternative is hashing the whole file
// a second time and that read is exactly what folding the part sums exists to
// remove. It cannot see a rewrite that preserves both, which is a real gap and
// the reason this is a guard on an upload that has already verified itself
// part by part rather than the verification itself.
func sourceMoved(before, after os.FileInfo) bool {
	return after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime())
}

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
//
// The whole-object checksum is folded from those same part sums rather than
// taken in a pass of its own. That pass used to run to completion before any
// part was sent, so by the time the parts were read its pages had been
// evicted: a cold read of the whole file whose only purpose was to know the
// value the service would later be asked to confirm.
//
// before is the stat the part geometry was computed from, and is what the file
// is compared against once its parts have been read. It is passed in rather
// than taken here on purpose: a stat of its own would leave a gap between the
// size the parts were planned for and the size they are checked against, and a
// file that grew inside that gap would upload only its original prefix with
// both stats agreeing that nothing had changed.
func (o *OCI) uploadMultipart(f *os.File, before os.FileInfo, spec, object string, partSize, parts int64, pb *bar.ProgressBar, gentle bool) error {
	ref, err := o.resolve(spec)
	if err != nil {
		return err
	}
	c, ns, bucket := ref.c, ref.ns, ref.name
	ctx := context.Background()
	size := before.Size()

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
	// Each part's own sum and length, kept in part order. Folded together
	// afterwards they are the checksum of the whole object, so the value the
	// service is asked to confirm costs nothing beyond the reads the parts
	// were going to do anyway.
	sums := make([]uint32, parts)
	lens := make([]int64, parts)
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
			//
			// This read is not paced even under --gentle-io, and that is the
			// point of doing it here: it pulls the part into the page cache
			// and the send that follows reads it back out again, so the part
			// costs one trip to the disk rather than two. The send is what
			// drops those pages afterwards.
			ph := crc32.New(tbl)
			read, cerr := io.Copy(ph, io.NewSectionReader(f, off, length))
			if cerr != nil {
				errs[i] = fmt.Errorf("oci: cannot read part %d of %s: %w", num, f.Name(), cerr)
				return
			}
			// A short read means the file was truncated under us: a section
			// reader past the new end simply stops, without an error. This is
			// here so that lens[i] below cannot record a length that was never
			// hashed, which would fold into the checksum of an object nobody
			// uploaded.
			//
			// It is not what usually reports a truncation, and the comment
			// should not pretend otherwise. Measured against the bucket: a
			// file halved two seconds into a 200 MiB upload had already been
			// hashed by then, and what failed was the body, one layer down --
			// "http: ContentLength=134217728 with Body length 104857600". The
			// hash read only sees it when the truncation lands in the moment
			// between this read and the send.
			if read != length {
				errs[i] = fmt.Errorf("oci: part %d of %s is short: read %d of %d bytes, so the file was truncated while it was being uploaded",
					num, f.Name(), read, length)
				return
			}
			partCRC := ph.Sum32()
			sums[i], lens[i] = partCRC, length
			partCRC64 := crc32cToBase64(partCRC)

			out, perr := c.UploadPart(ctx, objectstorage.UploadPartRequest{
				NamespaceName: &ns, BucketName: &bucket, ObjectName: &object,
				UploadId: uploadID, UploadPartNum: &num,
				ContentLength:        &length,
				UploadPartBody:       io.NopCloser(common.NewGentleSection(f, off, length, gentle, nil)),
				OpcChecksumAlgorithm: objectstorage.UploadPartOpcChecksumAlgorithmCrc32c,
				OpcContentCrc32c:     &partCRC64,
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

	// The whole object's checksum, from the parts' sums laid end to end. The
	// sums slice is in part order and is not what sort.Slice above touches.
	wholeCRC, total := common.FoldCRC32C(sums, lens)
	if total != size {
		return fmt.Errorf("oci: the parts of %s add up to %d bytes, not %d", f.Name(), total, size)
	}

	// The source must not have moved under the parts. Folding gives the
	// checksum of exactly the bytes that were sent, so a file rewritten
	// part-way through no longer disagrees with itself -- the object would be
	// stored, checksum and all, holding a mix of what the file was and what it
	// became. Reading the whole file again to notice is what this change
	// removed, so the cheap version is asked instead, and before the commit
	// rather than after it: nothing has been published yet, so the deferred
	// abort is the whole of the cleanup.
	//
	// Weaker than the hash it replaces, and deliberately so: a rewrite that
	// preserves both the size and the modification time is invisible to it.
	// That is the assumption the crc32c cache makes and that #57 and #60
	// refused to make about the bytes being *sent* -- here it decides only
	// whether to distrust an upload that has otherwise verified itself part by
	// part, which is a much smaller thing to be wrong about.
	after, serr := f.Stat()
	if serr != nil {
		return fmt.Errorf("oci: cannot measure %s after uploading its parts: %w", f.Name(), serr)
	}
	if sourceMoved(before, after) {
		return fmt.Errorf("oci: %s changed while its parts were being uploaded (%d bytes at %s, now %d bytes at %s): not committing an object assembled from two different files",
			f.Name(), before.Size(), before.ModTime(), after.Size(), after.ModTime())
	}

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
	// The checksum sent is now folded from the parts rather than read from the
	// file separately, so the two describe the same bytes by construction and
	// a source that moved mid-upload can no longer be what this catches --
	// that is the stat above, before anything is published. What is left is
	// the service's side of it: a commit that reports no checksum at all, or
	// one that disagrees because the assembly, or the response itself, was
	// faulty. Rarer, and still published by the time it can be seen, so the
	// removal stays.
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
