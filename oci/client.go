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

// bucketRef is a bucket with nothing left to infer: the client that reaches
// its region, the namespace it really lives in, and its name.
//
// Operations take this rather than three loose strings because the three have
// to agree. A client for one region with a bucket name from another addresses
// a bucket that does not exist, and the service reports that as an ordinary
// 404 -- indistinguishable, from the reply alone, from a bucket that was
// deleted.
type bucketRef struct {
	c      *objectstorage.ObjectStorageClient
	ns     string
	name   string
	region string
}

// String renders the reference the way a user would write it, so that a
// message about a bucket says which of the possibly several buckets of that
// name it means.
func (r bucketRef) String() string {
	return r.name + "@" + r.region
}

// sameBucket reports whether two resolved references name the same bucket.
//
// All three parts have to match. The namespace catches the two spellings of
// one bucket -- with and without an explicit namespace -- and the region
// catches the opposite mistake, two different buckets that happen to share a
// name. Comparing the raw path strings would get both wrong.
func (r bucketRef) sameBucket(other bucketRef) bool {
	return r.region == other.region && r.ns == other.ns && r.name == other.name
}

// configProvider reads the credentials, once.
//
// One provider serves every region. A request is signed with the tenancy's API
// key, and that key says nothing about where the request is going -- the
// region is carried by the endpoint the client dispatches to. So the config
// file is parsed once however many regions a command touches.
//
// The caller holds o.mu.
func (o *OCI) configProvider() ocicommon.ConfigurationProvider {
	if o.provider == nil {
		o.provider = ocicommon.DefaultConfigProvider()
	}
	return o.provider
}

// clientFor returns the client that talks to a region, building it on first
// use and caching it.
//
// Every client is built from the same credentials and then pointed at its own
// region. SetRegion is what makes that work: it rewrites the endpoint host for
// the region's realm, so one key reaches every region the tenancy is
// subscribed to.
//
// The region the config file carries is not used for routing -- the path
// decides that -- but it is still what the SDK validates the provider against
// when the client is constructed, which is why a config file with no region at
// all fails here rather than silently defaulting.
func (o *OCI) clientFor(region string) (*objectstorage.ObjectStorageClient, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.clientForLocked(region)
}

// clientForLocked is clientFor with o.mu already held.
func (o *OCI) clientForLocked(region string) (*objectstorage.ObjectStorageClient, error) {
	if c, ok := o.clients[region]; ok {
		return c, nil
	}
	c, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(o.configProvider())
	if err != nil {
		logger.Debug(module, "cannot build client for %s: %s", region, err)
		return nil, fmt.Errorf("oci: cannot build client for region %q: %w", region, err)
	}
	c.SetRegion(region)
	c.HTTPClient = newHTTPClient()
	if o.clients == nil {
		o.clients = map[string]*objectstorage.ObjectStorageClient{}
	}
	o.clients[region] = &c
	logger.Debug(module, "built a client for region %s", region)
	return &c, nil
}

// tenancyNamespace returns the namespace of the configured tenancy.
//
// It is asked for once and reused. A tenancy has exactly one namespace, it
// cannot change, and it is the same string in every region -- so which
// region's client answers the question does not matter, and a second region
// does not need to ask again.
//
// The region is named in the error because this is where a region the tenancy
// is not subscribed to first shows up, and it does not announce itself: the
// service answers 401 NotAuthenticated rather than anything about regions, so
// the bare SDK error reads as a credentials problem. Measured against a real
// tenancy, asking us-ashburn-1 from a tenancy subscribed only to
// ap-singapore-1 and us-phoenix-1 returns exactly that. The credentials are
// fine; they are simply not accepted there.
//
// The caller holds o.mu.
func (o *OCI) tenancyNamespace(c *objectstorage.ObjectStorageClient, region string) (string, error) {
	if o.namespace != "" {
		return o.namespace, nil
	}
	r, err := c.GetNamespace(context.Background(), objectstorage.GetNamespaceRequest{})
	if err != nil {
		logger.Debug(module, "cannot resolve namespace in %s: %s", region, err)
		return "", fmt.Errorf(
			"oci: cannot resolve the namespace in region %q (is the tenancy subscribed to it?): %w",
			region, err)
	}
	if r.Value == nil || *r.Value == "" {
		return "", fmt.Errorf("oci: tenancy returned an empty namespace")
	}
	o.namespace = *r.Value
	logger.Debug(module, "resolved namespace %s", o.namespace)
	return o.namespace, nil
}

// CanonicalBucket returns the single spelling of the bucket a path names.
//
// One bucket has two spellings -- "b@region" with the namespace left to the
// tenancy, and "b@namespace.region" with it written out -- and only this
// backend can tell that they are the same. Callers that must compare two paths
// without performing an operation on them ask here; cmd/mv.go is the one that
// has to, because its guard against a recursive move into its own descendant
// runs before any copy and so has nothing else to compare.
//
// The namespace is resolved only when the path omits it, and that answer is
// cached for the run, so the common case of two paths spelled the same way
// costs one GetNamespace at most and usually nothing.
func (o *OCI) CanonicalBucket(spec string) (string, error) {
	s, err := parseBucketSpec(spec)
	if err != nil {
		return "", err
	}
	if s.namespace == "" {
		o.mu.Lock()
		c, cerr := o.clientForLocked(s.region)
		if cerr != nil {
			o.mu.Unlock()
			return "", cerr
		}
		ns, nerr := o.tenancyNamespace(c, s.region)
		o.mu.Unlock()
		if nerr != nil {
			return "", nerr
		}
		s.namespace = ns
	}
	return s.name + "@" + s.namespace + "." + s.region, nil
}

// resolve turns the authority of a path into something addressable.
//
// It also establishes that the bucket exists, once. That matters for what a
// 404 on an object means: a HEAD has no response body, so the SDK reports a
// missing object, a missing bucket and a missing namespace identically, and
// the three cannot be told apart from the reply alone. Knowing the bucket is
// there beforehand is what makes a later 404 mean "no such object" -- so
// headObject does not have to ask about the bucket every time one is missing,
// which over a large listing would be a bucket lookup per absent object.
func (o *OCI) resolve(spec string) (bucketRef, error) {
	s, err := parseBucketSpec(spec)
	if err != nil {
		logger.Info(module, "%s", err)
		return bucketRef{}, err
	}

	o.mu.Lock()
	c, err := o.clientForLocked(s.region)
	if err != nil {
		o.mu.Unlock()
		return bucketRef{}, err
	}
	ns := s.namespace
	if ns == "" {
		// Resolved against this path's own client, so a run that only ever
		// touches one region never builds a client for another just to ask.
		if ns, err = o.tenancyNamespace(c, s.region); err != nil {
			o.mu.Unlock()
			return bucketRef{}, err
		}
	}
	o.mu.Unlock()

	r := bucketRef{c: c, ns: ns, name: s.name, region: s.region}
	if err = o.verifyBucket(r); err != nil {
		return bucketRef{}, err
	}
	return r, nil
}

// verifyBucket checks that a bucket exists, at most once per bucket.
func (o *OCI) verifyBucket(r bucketRef) error {
	return o.verifyBucketWith(r.region, r.ns, r.name, func() error {
		// GetBucket, not HeadBucket: a HEAD gives the same empty body the
		// object calls do, so its 404 carries no code. GetBucket answers with
		// BucketNotFound, which is worth having in the message.
		_, err := r.c.GetBucket(context.Background(), objectstorage.GetBucketRequest{
			NamespaceName: &r.ns, BucketName: &r.name,
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
func (o *OCI) verifyBucketWith(region, ns, bucket string, check func() error) error {
	key := region + "/" + ns + "/" + bucket

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
		logger.Debug(module, "cannot reach bucket oci://%s@%s: %s", bucket, region, cerr)
		err = fmt.Errorf("oci: cannot reach bucket %q in namespace %q in region %q: %w", bucket, ns, region, cerr)
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
