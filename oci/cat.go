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
// least one object strictly beneath it.
//
// An object at the path itself does not make it one -- "a/b.txt" is not a
// directory just because it exists -- so the listing is compared against the
// prefix rather than merely counted. A single entry equal to the prefix is the
// object itself; anything longer is something underneath.
func (o *OCI) IsDirectory(bucket, prefix string) (bool, error) {
	// Ask about the path as a directory. Without the slash, "a/bc.txt" would
	// count as being under "a/b" on a plain string prefix match.
	asDir := prefix
	if asDir != "" && !strings.HasSuffix(asDir, "/") {
		asDir += "/"
	}
	objects, prefixes, err := o.walkObjects(bucket, asDir, true)
	if err != nil {
		return false, err
	}
	if len(prefixes) > 0 {
		return true, nil
	}
	for i := range objects {
		if objects[i].Name == nil {
			continue
		}
		// A zero-length marker named exactly "a/" says a directory was meant
		// to exist, but on its own it holds nothing; treating it as a
		// directory keeps gsg consistent with the tools that write them.
		if len(*objects[i].Name) >= len(asDir) {
			return true, nil
		}
	}
	return false, nil
}
