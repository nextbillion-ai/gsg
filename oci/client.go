package oci

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/nextbillion-ai/gsg/logger"

	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// newHTTPClient builds the client the SDK dispatches through.
//
// The SDK's own default puts a 60-second deadline on the whole request, body
// included, and gsg stores an object in one PutObject. Anything taking longer
// than a minute to send was therefore cancelled mid-stream however healthy the
// connection was, so the largest object gsg could store was set by link speed
// rather than by any limit of the service: measured, 3 GiB and 6 GiB both
// failed at ~61s, and both stored fine once the deadline was moved.
//
// A whole-request deadline cannot work when the request carries the body --
// its duration legitimately scales with the file. Zero is Go's "no deadline",
// and is what both the aws and google SDKs ship, for exactly this reason. What
// actually has to be caught is a peer that stops responding, and the limits
// below do that at the transport level, where they hold however long the
// transfer runs. ResponseHeaderTimeout is the one that would be unsafe if it
// were measured from the start of the request; Go starts it only once the body
// has been fully written, so a twenty-minute upload cannot trip it while a
// server that goes silent afterwards still fails in a minute.
//
// The values match the aws and google defaults so the three backends behave
// alike.
func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ResponseHeaderTimeout: 60 * time.Second,
		},
	}
}

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
		c.HTTPClient = newHTTPClient()
		o.client = &c
		// Region comes from the same provider as the credentials. A copy needs
		// it on every request, so reading it once here avoids re-parsing the
		// config file per object.
		if reg, rerr := p.Region(); rerr == nil {
			o.region = reg
		} else {
			logger.Debug(module, "cannot read region from config: %s", rerr)
		}
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
//
// It also establishes that the bucket exists, once. That matters for what a
// 404 on an object means: a HEAD has no response body, so the SDK reports a
// missing object, a missing bucket and a missing namespace identically, and
// the three cannot be told apart from the reply alone. Knowing the bucket is
// there beforehand is what makes a later 404 mean "no such object" -- so
// headObject does not have to ask about the bucket every time one is missing,
// which over a large listing would be a bucket lookup per absent object.
func (o *OCI) resolve(bucketSpec string) (*objectstorage.ObjectStorageClient, string, string, error) {
	c, ns, err := o.clientAndNamespace(bucketSpec)
	if err != nil {
		return nil, "", "", err
	}
	name, _ := splitBucket(bucketSpec)
	if name == "" {
		return nil, "", "", fmt.Errorf("oci: no bucket given")
	}
	if err = o.verifyBucket(c, ns, name); err != nil {
		return nil, "", "", err
	}
	return c, ns, name, nil
}

// verifyBucket checks that a bucket exists, at most once per bucket.
func (o *OCI) verifyBucket(c *objectstorage.ObjectStorageClient, ns, bucket string) error {
	return o.verifyBucketWith(ns, bucket, func() error {
		// GetBucket, not HeadBucket: a HEAD gives the same empty body the
		// object calls do, so its 404 carries no code. GetBucket answers with
		// BucketNotFound, which is worth having in the message.
		_, err := c.GetBucket(context.Background(), objectstorage.GetBucketRequest{
			NamespaceName: &ns, BucketName: &bucket,
		})
		return err
	})
}

// verifyBucketWith is verifyBucket with the request itself supplied, so that
// what gets remembered and what does not can be tested without a live service.
//
// The lock is held across the check on purpose. Under -m the first operation
// on a bucket starts many workers at once, and releasing the lock before the
// call would send all of them to the service for the same answer.
func (o *OCI) verifyBucketWith(ns, bucket string, check func() error) error {
	key := ns + "/" + bucket

	o.mu.Lock()
	defer o.mu.Unlock()
	if err, known := o.buckets[key]; known {
		return err
	}

	var err error
	if cerr := check(); cerr != nil {
		// Logged at debug and returned, not logged at info: the command layer
		// prints whatever error it is handed, and an info line worded
		// differently from the error would be printed alongside it rather
		// than instead of it.
		logger.Debug(module, "cannot reach bucket oci://%s: %s", bucket, cerr)
		err = fmt.Errorf("oci: cannot reach bucket %q in namespace %q: %w", bucket, ns, cerr)
		// Only a definite answer is worth remembering. A throttle, a 5xx or a
		// dropped connection says nothing about whether the bucket exists, and
		// caching it would make the first unlucky moment of a run permanent:
		// common.DoWithRetrySimple would retry, get the remembered error back
		// without a request, and fail every time. So transient failures are
		// returned and forgotten, and the next attempt asks again.
		if !isNotFound(cerr) {
			return err
		}
	}
	if o.buckets == nil {
		o.buckets = map[string]error{}
	}
	o.buckets[key] = err
	return err
}
