package oci

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"

	"github.com/nextbillion-ai/gsg/logger"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// Cat returns an object's bytes.
func (o *OCI) Cat(bucket, prefix string) ([]byte, error) {
	c, ns, name, err := o.resolve(bucket)
	if err != nil {
		return nil, err
	}
	r, err := c.GetObject(context.Background(), objectstorage.GetObjectRequest{
		NamespaceName: &ns, BucketName: &name, ObjectName: &prefix,
	})
	if err != nil {
		logger.Info(module, "cannot read oci://%s/%s: %s", name, prefix, err)
		return nil, err
	}
	defer func() {
		if r.Content != nil {
			_ = r.Content.Close()
		}
	}()
	if r.Content == nil {
		return nil, nil
	}
	buf := new(bytes.Buffer)
	// ReadFrom, not ReadAll into a fixed buffer: a short read that is not
	// reported is how a truncated object comes back looking complete.
	if _, err = buf.ReadFrom(r.Content); err != nil {
		logger.Info(module, "cannot read the body of oci://%s/%s: %s", name, prefix, err)
		return nil, err
	}
	return buf.Bytes(), nil
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
	return o.anyEntryUnder(bucket, asDir)
}

// GetObjectReader streams an object's bytes.
//
// The library API (lib/object) reads through this rather than Cat, so that a
// large object is not held in memory in its entirety just to be copied
// somewhere else. The caller closes it.
func (o *OCI) GetObjectReader(bucket, prefix string) (io.ReadCloser, error) {
	c, ns, name, err := o.resolve(bucket)
	if err != nil {
		return nil, err
	}
	r, err := c.GetObject(context.Background(), objectstorage.GetObjectRequest{
		NamespaceName: &ns, BucketName: &name, ObjectName: &prefix,
	})
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
	c, ns, name, err := o.resolve(bucket)
	if err != nil {
		return err
	}

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
