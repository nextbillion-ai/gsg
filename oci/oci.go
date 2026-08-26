// Package oci implements the gsg storage interface on top of OCI Object
// Storage, alongside the existing gs and s3 backends.
//
// # Addressing
//
// OCI has no URL scheme of its own the way gs:// and s3:// do: an object is
// identified by a namespace, a bucket and a name, and Oracle's own tools spell
// that several different ways. gsg defines one so that an OCI path reads like
// any other path a gsg user already types.
//
//	oci://bucket/some/object          namespace discovered from the tenancy
//	oci://bucket@namespace/some/obj   namespace given explicitly
//
// The first form is the everyday one and is deliberately the same shape as
// s3://bucket/some/object. The namespace is a property of the tenancy rather
// than of the object, so it is looked up once per run and cached; it never has
// to appear in a command line unless you mean a namespace other than your own.
//
// The second form exists because that inference is only correct while you stay
// in one tenancy. It is also the spelling Oracle's own tooling uses, so it
// should look familiar to anyone arriving from OCI.
//
// Both round-trip through FileObject.GetFullPath: the parser stores the
// explicit form as "bucket@namespace" in Bucket, and splitBucket below is the
// only thing that needs to know that.
package oci

import (
	"fmt"
	"strings"
	"sync"

	"github.com/nextbillion-ai/gsg/logger"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

const module = "OCI"

// errNotImplemented is what every method that has not been built yet returns.
// The backend is registered from the start so that oci:// paths are recognised
// and report honestly, rather than being rejected as an unknown scheme.
//
// It logs as well as returning, because returning alone is not enough to reach
// the user: the command layer discards the error and calls common.Exit, which
// is a bare os.Exit(1). cmd/ls.go is the clearest example --
//
//	if objs, err = fo.System.List(...); err != nil {
//	    common.Exit()
//	}
//
// -- so without this the user gets exit 1 and an empty screen. The other
// backends happen to avoid that by logging further down, in the calls List
// itself makes; a stub has no such calls to log from.
func errNotImplemented(op string) error {
	err := fmt.Errorf("oci: %s is not implemented yet", op)
	logger.Info(module, "%s", err)
	return err
}

// OCI is the OCI Object Storage backend.
//
// The zero value is ready to use; the client and namespace are resolved on
// first use and reused after that.
type OCI struct {
	mu        sync.Mutex
	client    *objectstorage.ObjectStorageClient
	namespace string
	// buckets remembers the outcome of checking that a bucket exists, keyed
	// by "namespace/bucket". Existence is a property of the bucket, not of
	// each object in it, so it is established once and reused -- the same
	// shape as the s3 backend, where clientFor resolves a bucket's region
	// once and caches the client.
	buckets map[string]error
}

// Scheme returns the URL scheme this backend answers to.
func (o *OCI) Scheme() string {
	return "oci"
}

// Init resolves the client ahead of first use. The variadic bucket list is
// part of the shared interface and is unused here: unlike S3, one OCI client
// serves every bucket in a region, so there is nothing per-bucket to warm up.
func (o *OCI) Init(_ ...string) error {
	_, _, err := o.clientAndNamespace("")
	return err
}

// splitBucket separates "bucket@namespace" into its two parts.
//
// The generic parser cannot know that oci://bucket@namespace/x means anything
// in particular, so it keeps the whole authority in Bucket and leaves the
// interpretation to us. An empty namespace means "use the tenancy's own",
// which is the common case.
func splitBucket(bucket string) (name, namespace string) {
	if i := strings.LastIndex(bucket, "@"); i >= 0 {
		return bucket[:i], bucket[i+1:]
	}
	return bucket, ""
}
