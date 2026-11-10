package oci

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"

	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// Cat writes an object to w as it arrives.
//
// Every range is pinned to the ETag the HEAD saw with If-Match, as Download
// does. The stored CRC32C is checked when there is one, otherwise the MD5 of a
// single-part upload; a multipart MD5 is of the parts and cannot be.
func (o *OCI) Cat(bucket, prefix string, w io.Writer, opts common.StreamOptions) error {
	ref, err := o.resolve(bucket)
	if err != nil {
		return err
	}
	head, err := o.headObject(bucket, prefix)
	if err != nil {
		return err
	}
	if head == nil {
		// The GetObject error is what callers have always been shown for a
		// missing object, so it is asked for rather than paraphrased.
		r, gerr := ref.c.GetObject(context.Background(), objectstorage.GetObjectRequest{
			NamespaceName: &ref.ns, BucketName: &ref.name, ObjectName: &prefix,
		})
		if gerr == nil {
			if r.Content != nil {
				_ = r.Content.Close()
			}
			gerr = fmt.Errorf("oci: no object at oci://%s/%s", ref.name, prefix)
		}
		logger.Info(module, "%s", catFailure(ref.name, prefix, gerr))
		return gerr
	}
	var size int64
	if head.ContentLength != nil {
		size = *head.ContentLength
	}
	var pin *string
	if head.ETag != nil && *head.ETag != "" {
		pin = head.ETag
	}
	src := common.StreamSource{
		Name: fmt.Sprintf("oci://%s/%s", ref.name, prefix),
		Size: size,
		Open: func(ctx context.Context, off, n int64) (io.ReadCloser, error) {
			req := objectstorage.GetObjectRequest{
				NamespaceName: &ref.ns, BucketName: &ref.name, ObjectName: &prefix,
				IfMatch: pin,
			}
			if header, ranged := rangeHeader(off, n); ranged {
				req.Range = &header
			}
			r, gerr := ref.c.GetObject(ctx, req)
			if gerr != nil {
				return nil, gerr
			}
			if r.Content == nil {
				return nil, fmt.Errorf("oci: oci://%s/%s returned no body", ref.name, prefix)
			}
			return r.Content, nil
		},
		Permanent: permanentReadError,
	}
	src.CRC32C, src.HasCRC32C = crc32cOf(head.OpcContentCrc32c)
	if !src.HasCRC32C && head.ContentMd5 != nil {
		if sum, derr := base64.StdEncoding.DecodeString(*head.ContentMd5); derr == nil && len(sum) == md5.Size {
			src.MD5 = sum
		}
	}
	return common.Stream(context.Background(), w, src, opts)
}

// catFailure is the line cat prints when the object cannot be opened.
func catFailure(bucket, prefix string, err error) string {
	return fmt.Sprintf("cannot read oci://%s/%s: %s", bucket, prefix, err)
}

// permanentReadError reports a read that no retry can turn into the pinned
// version's bytes.
func permanentReadError(err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}
	var se ocicommon.ServiceError
	if asServiceError(err, &se) {
		switch se.GetHTTPStatusCode() {
		case 400, 401, 403, 404, 412, 416:
			return true
		}
	}
	return false
}

// IsObject reports whether prefix names an object.
//
// A failure is an error, never a false. The distinction is the one TODO items
// 3 and 14 were about: callers treat "not an object" as "nothing is there",
// and on the s3 side a request that could not be answered therefore read as
// absence. headObject already separates the two.
func (o *OCI) IsObject(bucket, prefix string) (bool, error) {
	r, err := o.headObject(bucket, prefix)
	if err != nil {
		return false, err
	}
	return r != nil, nil
}

// IsDirectory reports whether prefix names a directory: something with at
// least one object beneath it.
//
// The trailing slash is what makes the question mean that. Without it the
// service matches on the raw prefix, so "edge/ab" would look like a directory
// because "edge/abc.txt" exists, and "edge/d" like one because "edge/dir/"
// does.
//
// An object at the path itself does not make it a directory: asking about
// "a/b.txt" appends the slash, and nothing lives under "a/b.txt/".
func (o *OCI) IsDirectory(bucket, prefix string) (bool, error) {
	asDir := prefix
	if asDir != "" && !strings.HasSuffix(asDir, "/") {
		asDir += "/"
	}
	// The caller's own prefix is what must not count as something beneath it,
	// so it is passed through rather than asDir: that is what makes "foo" and
	// "foo/" answer differently for a lone marker, matching gs and s3.
	return o.anyEntryUnder(bucket, asDir, prefix)
}

// GetObjectReader streams an object's bytes.
//
// The library API (lib/object) reads through this rather than Cat, so that a
// large object is not held in memory in its entirety just to be copied
// somewhere else. The caller closes it.
func (o *OCI) GetObjectReader(bucket, prefix string) (io.ReadCloser, error) {
	return o.getObjectReader(bucket, prefix, 0, -1)
}

// GetObjectRangeReader streams part of an object: length bytes from offset, or
// everything from offset on when length is negative.
//
// Download fetches an object as parallel ranges rather than one stream, and
// this is the same capability offered to a library caller, which could not
// otherwise assemble it -- GetObjectReader takes no offset, so anything
// wanting a part of an object had to read up to it and throw the rest away.
//
// A length of zero yields nothing, and does so without asking the service.
// Passing it through would be worse than useless: an absent range header means
// "the whole object", which is exactly what the unranged reader relies on it
// meaning, so a caller asking for no bytes would be handed all of them.
func (o *OCI) GetObjectRangeReader(bucket, prefix string, offset, length int64) (io.ReadCloser, error) {
	if length == 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	return o.getObjectReader(bucket, prefix, offset, length)
}

func (o *OCI) getObjectReader(bucket, prefix string, offset, length int64) (io.ReadCloser, error) {
	ref, err := o.resolve(bucket)
	if err != nil {
		return nil, err
	}
	c, ns, name := ref.c, ref.ns, ref.name
	req := objectstorage.GetObjectRequest{
		NamespaceName: &ns, BucketName: &name, ObjectName: &prefix,
	}
	if header, ranged := rangeHeader(offset, length); ranged {
		req.Range = &header
	}
	r, err := c.GetObject(context.Background(), req)
	if err != nil {
		logger.Info(module, "cannot read oci://%s/%s: %s", name, prefix, err)
		return nil, err
	}
	if r.Content == nil {
		return nil, fmt.Errorf("oci: oci://%s/%s returned no body", name, prefix)
	}
	return r.Content, nil
}

// PutObject stores what the reader yields.
//
// Unlike Upload it takes a reader rather than a path, which is what the
// library API needs.
//
// The body is measured before it is sent, because both things worth having
// need to be known up front: the content length, and the CRC32C the service
// checks the arriving bytes against. Reading it all into memory to do that is
// what the first version did, and lib/object.Write accepts arbitrary streams,
// so a large object could exhaust memory before a byte was sent -- the s3
// backend streams and does not have that problem.
//
// So a seekable reader is measured in place and rewound, and anything else is
// spooled to a temporary file first. Either way memory stays bounded and the
// upload is still checked on arrival.
func (o *OCI) PutObject(bucket, prefix string, from io.Reader) error {
	ref, err := o.resolve(bucket)
	if err != nil {
		return err
	}
	c, ns, name := ref.c, ref.ns, ref.name

	body, size, sum, cleanup, err := measureBody(from)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		logger.Info(module, "cannot read the body for oci://%s/%s: %s", name, prefix, err)
		return err
	}

	if _, err = c.PutObject(context.Background(), objectstorage.PutObjectRequest{
		NamespaceName: &ns, BucketName: &name, ObjectName: &prefix,
		ContentLength: &size, PutObjectBody: io.NopCloser(body),
		OpcChecksumAlgorithm: objectstorage.PutObjectOpcChecksumAlgorithmCrc32c,
		OpcContentCrc32c:     &sum,
	}); err != nil {
		logger.Info(module, "cannot write oci://%s/%s: %s", name, prefix, err)
		return err
	}
	return nil
}

// measureBody returns a reader positioned at the start, its length, and its
// CRC32C, without holding the whole thing in memory.
//
// The returned cleanup, when not nil, must be called: it removes the spool
// file for a reader that could not be rewound.
func measureBody(from io.Reader) (body io.Reader, size int64, sum string, cleanup func(), err error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	// A reader that can rewind needs no copy at all: hash it where it is and
	// go back to the start. This covers what callers usually pass -- a
	// bytes.Reader, a strings.Reader, an open file.
	if s, ok := from.(io.ReadSeeker); ok {
		start, serr := s.Seek(0, io.SeekCurrent)
		if serr == nil {
			n, cerr := io.Copy(h, s)
			if cerr != nil {
				return nil, 0, "", nil, cerr
			}
			if _, serr = s.Seek(start, io.SeekStart); serr == nil {
				return s, n, crc32cToBase64(h.Sum32()), nil, nil
			}
			// Hashed but could not rewind, so the bytes are gone. Fall through
			// rather than upload something that cannot be re-read.
			return nil, 0, "", nil, fmt.Errorf("oci: cannot rewind the body after measuring it: %w", serr)
		}
	}

	// Otherwise spool to disk. Bounded memory, at the cost of a temporary
	// file the size of the object.
	f, err := os.CreateTemp("", "gsg-oci-put-*")
	if err != nil {
		return nil, 0, "", nil, err
	}
	cleanup = func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}
	n, err := io.Copy(io.MultiWriter(f, h), from)
	if err != nil {
		return nil, 0, "", cleanup, err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, "", cleanup, err
	}
	return f, n, crc32cToBase64(h.Sum32()), cleanup, nil
}
