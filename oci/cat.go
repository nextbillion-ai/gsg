package oci

import (
	"bytes"
	"context"
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
