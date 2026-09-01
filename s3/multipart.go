package s3

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/nextbillion-ai/gsg/bar"
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
)

const (
	// s3MinPartSize is what S3 requires of every part but the last.
	s3MinPartSize int64 = 5 * 1024 * 1024

	// s3MaxPartSize is the largest a single part may be.
	s3MaxPartSize int64 = 5 * 1024 * 1024 * 1024

	// s3MaxSinglePut is the largest object a single PutObject accepts.
	// Measured: a 6 GiB PutObject is refused in 0s with EntityTooLarge, before
	// any bytes move. Above this, multipart is the only way to store an object
	// at all -- it is not an optimisation.
	s3MaxSinglePut int64 = 5 * 1024 * 1024 * 1024

	// s3MultipartThreshold is where multipart starts being worth its extra
	// round trips, well below the point where it becomes mandatory.
	s3MultipartThreshold int64 = 128 * 1024 * 1024
)

// uploadMultipart stores srcFile as one object assembled from parts.
//
// The checksum is the reason this is hand-rolled rather than handed to
// feature/s3/manager. A multipart upload stores a COMPOSITE checksum by
// default -- a checksum of the parts' checksums -- which crc32cOf rightly
// refuses, since it cannot be compared against a whole-file CRC32C. Every
// object gsg uploaded that way would read back as "no comparable checksum" and
// rsync would copy it again on every run.
//
// Asking for ChecksumType FULL_OBJECT avoids that entirely: the service stores
// the whole-object CRC32C instead. Verified against the real service -- a
// two-part upload read back ChecksumType=FULL_OBJECT with a CRC32C identical
// to the locally computed whole-file value, which is exactly what crc32cOf
// accepts. So nothing on the read side changes.
func (s *S3) uploadMultipart(f *os.File, size int64, bucket, prefix string, partSize, parts int64, pb *bar.ProgressBar) error {
	c, err := s.clientFor(bucket)
	if err != nil {
		return err
	}
	ctx := context.TODO()

	// The whole-file checksum, from the handle the parts are read from, so it
	// cannot describe different bytes than the upload sends.
	wholeCRC, err := crc32cOfFile(f)
	if err != nil {
		return err
	}

	create, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(prefix),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
		ChecksumType:      types.ChecksumTypeFullObject,
	})
	if err != nil {
		logger.Info(module, "cannot start a multipart upload of s3://%s/%s: %s", bucket, prefix, err)
		return err
	}
	uploadID := create.UploadId

	// An upload left neither completed nor aborted keeps billing for the parts
	// already stored, so every path out of here past this point has to abort
	// -- including a panic, which is why this is deferred on a flag rather
	// than called at each return.
	completed := false
	abort := func() {
		if _, aerr := c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(prefix), UploadId: uploadID,
		}); aerr != nil {
			logger.Info(module, "could not abort the multipart upload of s3://%s/%s; its parts will bill until removed, by a lifecycle rule or by hand: %s", bucket, prefix, aerr)
		}
	}

	defer func() {
		if !completed {
			abort()
		}
	}()

	done := make([]types.CompletedPart, parts)
	errs := make([]error, parts)
	sem := make(chan struct{}, common.PartConcurrency(parts))
	var wg sync.WaitGroup

	for i := int64(0); i < parts; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			// A panic in one part would otherwise take the process down with
			// the upload still open, leaving its parts to bill. Turn it into
			// an error so the deferred abort above runs.
			defer func() {
				if r := recover(); r != nil {
					errs[i] = fmt.Errorf("s3: panic uploading part %d: %v", i+1, r)
				}
			}()
			sem <- struct{}{}
			defer func() { <-sem }()

			num := int32(i + 1)
			off := i * partSize
			length := partSize
			if off+length > size {
				length = size - off
			}
			// A section reader per part: nothing is buffered, and each part
			// stays independently seekable, so the SDK can rewind and retry it
			// without the whole transfer starting again.
			body := io.NewSectionReader(f, off, length)
			out, perr := c.UploadPart(ctx, &s3.UploadPartInput{
				Bucket: aws.String(bucket), Key: aws.String(prefix),
				UploadId: uploadID, PartNumber: &num,
				Body:              body,
				ContentLength:     &length,
				ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
			})
			if perr != nil {
				logger.Info(module, "part %d of s3://%s/%s failed: %s", num, bucket, prefix, perr)
				errs[i] = perr
				return
			}
			if pb != nil {
				// Advanced a whole part at a time. Parts finish out of order,
				// so this is progress by completion rather than by bytes on
				// the wire, which is the only ordering that makes sense here.
				pb.IncrBy(length)
			}
			done[i] = types.CompletedPart{
				PartNumber: &num, ETag: out.ETag, ChecksumCRC32C: out.ChecksumCRC32C,
			}
		}(i)
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	// Parts must be listed in order.
	sort.Slice(done, func(a, b int) bool { return *done[a].PartNumber < *done[b].PartNumber })

	wholeB64 := crc32cToBase64(wholeCRC)
	if _, err = c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(prefix), UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: done},
		// The whole-object checksum and the size are what make the stored
		// object carry a FULL_OBJECT CRC32C rather than a composite one.
		ChecksumCRC32C: aws.String(wholeB64),
		ChecksumType:   types.ChecksumTypeFullObject,
		MpuObjectSize:  &size,
	}); err != nil {
		logger.Info(module, "cannot complete the multipart upload of s3://%s/%s: %s", bucket, prefix, err)
		return err
	}
	// Unlike oci, no post-hoc checksum comparison is needed: the whole-object
	// CRC32C went out with the complete request, so the service itself refuses
	// to assemble an object that does not match it.
	completed = true
	logger.Info(module, "uploaded s3://%s/%s in %d parts of %d bytes", bucket, prefix, parts, partSize)
	return nil
}

// crc32cToBase64 is the wire form of a checksum: four big-endian bytes, base64
// encoded, which is how S3 both reports and accepts one.
func crc32cToBase64(v uint32) string {
	raw := make([]byte, 4)
	binary.BigEndian.PutUint32(raw, v)
	return base64.StdEncoding.EncodeToString(raw)
}

// crc32cOfFile hashes everything f holds and rewinds it.
func crc32cOfFile(f *os.File) (uint32, error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	if _, err := io.Copy(h, f); err != nil {
		return 0, fmt.Errorf("s3: cannot read %s to checksum it: %w", f.Name(), err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("s3: cannot rewind %s after checksumming it: %w", f.Name(), err)
	}
	return h.Sum32(), nil
}
