package s3

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
)

// s3MaxCopySource is the largest object a single CopyObject accepts. Measured
// on a 30.97 GB object: refused in under 3s with "The specified copy source is
// larger than the maximum allowable size for a copy source: 5368709120", having
// written nothing.
const s3MaxCopySource int64 = 5 * 1024 * 1024 * 1024

// copyMultipart copies an object too large for CopyObject, using UploadPartCopy
// so that no bytes pass through this machine.
//
// dst is the client for dstBucket; the requests that describe the source go to
// its own bucket's client, since the two may be in different regions.
//
// CopyObject does three things on its own that have to be spelled out here, or
// an object would silently come out different depending on its size: it keeps
// the source's system properties and user metadata, it keeps its tags, and it
// reads one version of the source. The first two are carried over by hand, and
// reading the tags means this needs s3:GetObjectTagging even for a source with
// none. A copy that cannot read them fails rather than quietly losing them.
//
// The third holds for the content: every part must come from the ETag read at
// the start, so a source replaced mid-copy fails the copy rather than yielding
// an object assembled from two versions. It does not hold for the metadata and
// tags, which are separate reads, so a change to only those during the copy can
// leave the result with either.
func (s *S3) copyMultipart(dst *s3.Client, srcBucket, srcPrefix, dstBucket, dstPrefix string) error {
	// CopyObject refuses this too. Rewriting an object over itself in parts
	// would succeed, at a real cost, while changing nothing.
	if srcBucket == dstBucket && srcPrefix == dstPrefix {
		return common.Caused(fmt.Sprintf("s3: cannot copy s3://%s/%s onto itself", srcBucket, srcPrefix), common.ErrInvalidRequest)
	}
	src, err := s.clientFor(srcBucket)
	if err != nil {
		return err
	}
	ctx := context.TODO()

	head, err := src.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(srcBucket), Key: aws.String(srcPrefix),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		logger.Info(module, "cannot read s3://%s/%s to copy it: %s", srcBucket, srcPrefix, err)
		return err
	}
	if head.ContentLength == nil || head.ETag == nil {
		return fmt.Errorf("s3: s3://%s/%s reported no size or no ETag, so it cannot be copied in parts", srcBucket, srcPrefix)
	}
	size := *head.ContentLength

	tagging, err := sourceTagging(ctx, src, srcBucket, srcPrefix)
	if err != nil {
		logger.Info(module, "cannot read the tags of s3://%s/%s to copy them: %s", srcBucket, srcPrefix, err)
		return err
	}

	// No caller preference: Copy takes no run context, and a chunk size asked
	// for on the command line describes transfers rather than this.
	partSize, parts := common.PartGeometry(size, -1, s3MinPartSize, s3MaxPartSize)

	create, err := dst.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:                  aws.String(dstBucket),
		Key:                     aws.String(dstPrefix),
		ChecksumAlgorithm:       types.ChecksumAlgorithmCrc32c,
		ChecksumType:            types.ChecksumTypeFullObject,
		CacheControl:            head.CacheControl,
		ContentDisposition:      head.ContentDisposition,
		ContentEncoding:         head.ContentEncoding,
		ContentLanguage:         head.ContentLanguage,
		ContentType:             head.ContentType,
		Expires:                 head.Expires,
		Metadata:                head.Metadata,
		Tagging:                 tagging,
		WebsiteRedirectLocation: head.WebsiteRedirectLocation,
	})
	if err != nil {
		logger.Info(module, "cannot start a multipart copy to s3://%s/%s: %s", dstBucket, dstPrefix, err)
		return err
	}
	uploadID := create.UploadId
	logger.Info(module, "copying s3://%s/%s to s3://%s/%s in %d parts of %d bytes", srcBucket, srcPrefix, dstBucket, dstPrefix, parts, partSize)

	// As in uploadMultipart: the parts bill until this is completed or aborted,
	// so every path out from here has to do one or the other.
	completed := false
	defer func() {
		if !completed {
			abortCopy(ctx, dst, dstBucket, dstPrefix, uploadID)
		}
	}()

	done := make([]types.CompletedPart, parts)
	sums := make([]uint32, parts)
	lens := make([]int64, parts)

	// One failed part fails the copy, so no more are started. Those already in
	// flight are left to finish rather than cancelled: cancelling only drops
	// the connection, the service carries on with the part regardless, and an
	// abort racing a part in progress is how a failed copy left an upload open
	// and billing, with parts stored, on a real bucket.
	stop := make(chan struct{})
	var failOnce sync.Once
	var firstErr error
	fail := func(e error) {
		failOnce.Do(func() {
			firstErr = e
			close(stop)
		})
	}

	source := aws.String(copySourceHeader(srcBucket, srcPrefix))
	sem := make(chan struct{}, common.PartConcurrency(parts))
	var wg sync.WaitGroup
	for i := int64(0); i < parts; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					fail(common.PanicError(fmt.Sprintf("s3: panic copying part %d", i+1), r))
				}
			}()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-stop:
				return
			}
			// Both can be ready at once, and select picks either.
			select {
			case <-stop:
				return
			default:
			}

			num := int32(i + 1)
			off := i * partSize
			length := partSize
			if off+length > size {
				length = size - off
			}
			out, perr := dst.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
				Bucket: aws.String(dstBucket), Key: aws.String(dstPrefix),
				UploadId: uploadID, PartNumber: &num,
				CopySource:        source,
				CopySourceRange:   aws.String(fmt.Sprintf("bytes=%d-%d", off, off+length-1)),
				CopySourceIfMatch: head.ETag,
			})
			if perr != nil {
				logger.Info(module, "part %d of the copy to s3://%s/%s failed: %s", num, dstBucket, dstPrefix, perr)
				fail(perr)
				return
			}
			if out.CopyPartResult == nil || out.CopyPartResult.ETag == nil || out.CopyPartResult.ChecksumCRC32C == nil {
				fail(fmt.Errorf("s3: part %d of the copy to s3://%s/%s came back without an ETag or checksum", num, dstBucket, dstPrefix))
				return
			}
			sum, ok := decodeCRC32C(*out.CopyPartResult.ChecksumCRC32C)
			if !ok {
				fail(fmt.Errorf("s3: part %d of the copy to s3://%s/%s came back with an unreadable checksum %q", num, dstBucket, dstPrefix, *out.CopyPartResult.ChecksumCRC32C))
				return
			}
			sums[i], lens[i] = sum, length
			done[i] = types.CompletedPart{
				PartNumber: &num, ETag: out.CopyPartResult.ETag, ChecksumCRC32C: out.CopyPartResult.ChecksumCRC32C,
			}
		}(i)
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	sort.Slice(done, func(a, b int) bool { return *done[a].PartNumber < *done[b].PartNumber })

	// The service reports a checksum for each part it copied, and those fold
	// into the checksum of the whole. Where the source has a comparable one the
	// two must agree, and this is the point to find out: nothing is published
	// until the complete request below.
	whole, total := common.FoldCRC32C(sums, lens)
	if total != size {
		return fmt.Errorf("s3: the parts of the copy to s3://%s/%s total %d bytes, the source has %d", dstBucket, dstPrefix, total, size)
	}
	if srcCRC, ok := headCRC32C(head); ok && srcCRC != whole {
		return fmt.Errorf("s3: the copy to s3://%s/%s has CRC32C %08x, the source has %08x", dstBucket, dstPrefix, whole, srcCRC)
	}

	if _, err = dst.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(dstBucket), Key: aws.String(dstPrefix), UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: done},
		ChecksumCRC32C:  aws.String(crc32cToBase64(whole)),
		ChecksumType:    types.ChecksumTypeFullObject,
		MpuObjectSize:   &size,
	}); err != nil {
		logger.Info(module, "cannot complete the multipart copy to s3://%s/%s: %s", dstBucket, dstPrefix, err)
		return err
	}
	completed = true
	logger.Info(module, "copied s3://%s/%s to s3://%s/%s in %d parts", srcBucket, srcPrefix, dstBucket, dstPrefix, parts)
	return nil
}

// How hard a failed copy tries to confirm its abort. The deadline is what bounds
// it: this holds a pool worker, and each request can spend its own retries and
// network timeouts on top of the backoff. Variables so a test need not sit
// through them.
var (
	abortCopyAttempts = 5
	abortCopyBackoff  = time.Second
	abortCopyDeadline = 2 * time.Minute
)

// abortCopy aborts a multipart copy and confirms that it took.
//
// A successful abort is not proof on its own. The service documents that a part
// in progress may still land, that an abort may need repeating, and that listing
// the parts is how to tell; and measured, an upload stayed open with 6 parts well
// over a minute after a copy failed. So this asks until the upload is gone, and
// says so plainly if it never goes.
func abortCopy(parent context.Context, c *s3.Client, bucket, key string, uploadID *string) {
	ctx, cancel := context.WithTimeout(parent, abortCopyDeadline)
	defer cancel()
	var last error
retrying:
	for attempt := 0; attempt < abortCopyAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(abortCopyBackoff << (attempt - 1)):
			case <-ctx.Done():
				last = ctx.Err()
				break retrying
			}
		}
		if _, err := c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: uploadID,
		}); err != nil && !isNoSuchUpload(err) {
			last = err
			continue
		}
		_, err := c.ListParts(ctx, &s3.ListPartsInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: uploadID,
		})
		if isNoSuchUpload(err) {
			logger.Debug(module, "aborted the multipart copy to s3://%s/%s", bucket, key)
			return
		}
		if err != nil {
			last = err
			continue
		}
		last = errors.New("the upload is still open after aborting it")
	}
	logger.Info(module, "could not abort the multipart copy to s3://%s/%s (upload id %s); its parts will bill until removed, by a lifecycle rule or by hand: %s", bucket, key, aws.ToString(uploadID), last)
}

// isNoSuchUpload reports whether err says the multipart upload is not there.
func isNoSuchUpload(err error) bool {
	var ae smithy.APIError
	return errors.As(err, &ae) && ae.ErrorCode() == "NoSuchUpload"
}

// headCRC32C returns the whole-object CRC32C a HeadObject reported, under the
// same rule as crc32cOf: a composite checksum describes the parts, so it cannot
// be compared against the checksum of the bytes.
func headCRC32C(head *s3.HeadObjectOutput) (uint32, bool) {
	if head.ChecksumType != types.ChecksumTypeFullObject || head.ChecksumCRC32C == nil {
		return 0, false
	}
	return decodeCRC32C(*head.ChecksumCRC32C)
}

// sourceTagging returns an object's tags in the form a request carries them, or
// nil if it has none. A failure to read them is a failure of the copy, as it is
// for CopyObject, rather than a copy that quietly lost its tags.
func sourceTagging(ctx context.Context, c *s3.Client, bucket, key string) (*string, error) {
	out, err := c.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	if len(out.TagSet) == 0 {
		return nil, nil
	}
	values := url.Values{}
	for _, t := range out.TagSet {
		values.Set(aws.ToString(t.Key), aws.ToString(t.Value))
	}
	return aws.String(values.Encode()), nil
}

// copySourceHeader is the source reference for a copy, URL-encoded as the
// service requires. It decodes this header, so a raw "+" reads as a space and a
// raw "%20" as a space too, which names some other key entirely.
//
// url.PathEscape will not do: "+" is legal in a path, so it leaves that alone,
// and that is the character that does the damage. QueryEscape encodes it, and
// writes a space as "+", which is put right afterwards.
func copySourceHeader(bucket, key string) string {
	segments := strings.Split(key, "/")
	for i, seg := range segments {
		segments[i] = strings.ReplaceAll(url.QueryEscape(seg), "+", "%20")
	}
	return bucket + "/" + strings.Join(segments, "/")
}
