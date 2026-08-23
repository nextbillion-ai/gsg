package oci

import (
	"context"
	"fmt"

	"github.com/nextbillion-ai/gsg/logger"

	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// clientAndNamespace returns the shared client and the namespace to address.
//
// Both are resolved once and cached. One OCI client covers every bucket in the
// configured region, so unlike the S3 backend -- which needs a client per
// bucket because a bucket's region is discovered per bucket -- there is
// nothing to key a cache on.
//
// bucketSpec is the raw Bucket field, which may carry an explicit namespace as
// "bucket@namespace". An explicit namespace wins; otherwise the tenancy's own
// is looked up once and reused. Passing "" asks for the tenancy's own.
func (o *OCI) clientAndNamespace(bucketSpec string) (*objectstorage.ObjectStorageClient, string, error) {
	_, explicit := splitBucket(bucketSpec)

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.client == nil {
		p := ocicommon.DefaultConfigProvider()
		c, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(p)
		if err != nil {
			logger.Debug(module, "cannot build client: %s", err)
			return nil, "", fmt.Errorf("oci: cannot build client: %w", err)
		}
		o.client = &c
	}

	// An explicit namespace needs no lookup, and must not overwrite the cached
	// one: a single run may legitimately touch both its own tenancy and
	// another, and the cache is what the bucket-only form falls back on.
	if explicit != "" {
		return o.client, explicit, nil
	}

	if o.namespace == "" {
		r, err := o.client.GetNamespace(context.Background(), objectstorage.GetNamespaceRequest{})
		if err != nil {
			logger.Debug(module, "cannot resolve namespace: %s", err)
			return nil, "", fmt.Errorf("oci: cannot resolve namespace: %w", err)
		}
		if r.Value == nil || *r.Value == "" {
			return nil, "", fmt.Errorf("oci: tenancy returned an empty namespace")
		}
		o.namespace = *r.Value
		logger.Debug(module, "resolved namespace %s", o.namespace)
	}
	return o.client, o.namespace, nil
}

// resolve is the form every operation wants: the client, the namespace, and
// the bucket name with any "@namespace" suffix already stripped off.
func (o *OCI) resolve(bucketSpec string) (*objectstorage.ObjectStorageClient, string, string, error) {
	c, ns, err := o.clientAndNamespace(bucketSpec)
	if err != nil {
		return nil, "", "", err
	}
	name, _ := splitBucket(bucketSpec)
	if name == "" {
		return nil, "", "", fmt.Errorf("oci: no bucket given")
	}
	return c, ns, name, nil
}
