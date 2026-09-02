// Package oci implements the gsg storage interface on top of OCI Object
// Storage, alongside the existing gs and s3 backends.
//
// # Addressing
//
// OCI has no URL scheme of its own the way gs:// and s3:// do: an object is
// identified by a region, a namespace, a bucket and a name, and Oracle's own
// tools spell that several different ways. gsg defines one spelling that
// carries all four.
//
//	oci://bucket@ap-singapore-1/some/object            namespace from the tenancy
//	oci://bucket@axkm4tp1h2ba.ap-singapore-1/some/obj  namespace given explicitly
//
// The region is mandatory, and that is the one place this backend deliberately
// reads differently from s3://bucket/some/object. S3 gets to leave the region
// out because a bucket's region is discoverable -- s3manager.GetBucketRegion
// asks the service, which is what s3/s3.go does per bucket. OCI has no
// equivalent call: a bucket resource carries no region field at all, because
// its region is decided by which regional endpoint answered. So a path without
// a region is not resolved by inference, it is resolved by whatever region
// happens to be configured on the machine running the command -- which makes
// the same path mean different things in different places. For a tool whose
// Move is copy-then-delete, that is not a tradeoff worth making, so the region
// is either in the path or the path is rejected.
//
// The namespace stays optional because it genuinely is inferable. A tenancy
// has exactly one namespace, permanently, and it is the same in every region,
// so it is looked up once per run and cached. It only has to be spelled out
// when the bucket belongs to another tenancy, which is also the spelling
// Oracle's own tooling uses.
//
// Ordering is narrow to wide -- bucket, then namespace, then region -- which
// is how the service itself nests them: the REST path is /n/{namespace}/b/{bucket}
// under a regional host. Where nothing follows a dot the whole qualifier is
// the region, since the region is the mandatory one; where a dot is present,
// the region is what follows the last of them. Splitting on the last rather
// than the first matters because a region name never contains a dot but a
// namespace may: most are alphanumeric, but a few older tenancies carry
// underscores, dashes or periods.
//
// One region has exactly one spelling. Short names ("sin", "iad") are rejected
// in favour of the full form, and so is anything but lower case -- folding
// either would make two paths that name one bucket look different to
// cmd/mv.go, whose guard against a recursive move onto its own descendant
// compares the bucket as written (TODO.md item 25).
//
// Short region names are rejected in favour of the full form for a second
// reason too.
// They look safer than they are: of the 78 short codes the SDK knows, 18 pairs
// are one keystroke apart -- sin/lin, sin/snn, iad/mad, hyd/syd among them --
// so a slip has a real chance of naming a different live region, passing
// validation, and surfacing much later as a missing bucket. A slip in
// "ap-singapore-1" almost always produces something that is not a region at
// all, and is rejected where it was written.
//
// All of this round-trips through FileObject.GetFullPath: the generic parser
// in common/url.go keeps the whole authority in Bucket, and parseBucketSpec
// below is the only thing that interprets it.
//
// # Regions and credentials
//
// One process serves any number of regions. Credentials are read once, and a
// client is built per region on first use, so a single command may name
// buckets in several regions and a copy may cross between them.
//
// The region in ~/.oci/config is no longer what gsg routes by, but it is still
// required: the SDK refuses to build a client from a provider that cannot
// answer Region(), whatever gsg does with the client afterwards. Existing
// config files therefore keep working unchanged; their region simply stops
// deciding where objects are read and written.
package oci

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/nextbillion-ai/gsg/logger"

	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
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
// The zero value is ready to use; credentials, clients and the namespace are
// resolved on first use and reused after that.
type OCI struct {
	mu sync.Mutex
	// provider carries the credentials, and is shared by every client. One
	// read of ~/.oci/config serves all regions: a region is a property of the
	// endpoint, not of the key that signs the request.
	provider ocicommon.ConfigurationProvider
	// clients is one client per region, built on demand. This is the same
	// shape as the s3 backend's per-bucket client cache, keyed one level up:
	// on S3 a bucket's region has to be discovered, so the cache is per
	// bucket; here the region is in the path, so every bucket in a region can
	// share a client.
	clients map[string]*objectstorage.ObjectStorageClient
	// namespace is the tenancy's own, used whenever a path does not name one.
	// A tenancy has exactly one namespace and it is identical in every region,
	// so this stays a single value however many regions are in play.
	namespace string
	// buckets remembers the outcome of checking that a bucket exists, keyed by
	// "region/namespace/bucket". Existence is a property of the bucket, not of
	// each object in it, so it is established once and reused. The region is
	// part of the key because it is part of the identity: the same name in two
	// regions is two different buckets.
	buckets map[string]error
}

// Scheme returns the URL scheme this backend answers to.
func (o *OCI) Scheme() string {
	return "oci"
}

// Init warms a client for each region named in the given buckets.
//
// The variadic list used to be ignored, because one client served everything.
// Now that a client is per region it is exactly the right input: each entry
// names its own region, so the work of reading credentials and building
// clients happens once here rather than racing on the first parallel
// operation. An entry that does not parse is left for the operation itself to
// report, where the message can name what the user was trying to do.
func (o *OCI) Init(buckets ...string) error {
	for _, b := range buckets {
		s, err := parseBucketSpec(b)
		if err != nil {
			continue
		}
		if _, err = o.clientFor(s.region); err != nil {
			return err
		}
	}
	return nil
}

// bucketSpec is the parsed authority of an oci:// path: everything the path
// says about which bucket is meant, before anything is resolved against the
// service.
type bucketSpec struct {
	name string
	// namespace is empty when the path did not name one, meaning "the
	// tenancy's own".
	namespace string
	region    string
}

// regionShape matches the canonical form of an OCI region name: an area, one
// or more words, and a number -- "ap-singapore-1", and also the four-part
// government forms such as "us-gov-ashburn-1".
//
// It is a shape check, not a membership test. The SDK has no exported list of
// regions to test against, and hard-coding one in gsg would go stale every
// time Oracle opens a region -- rejecting a region that genuinely exists is a
// worse failure than accepting a typo, because the user has no way to proceed.
// What this does catch is the shape of a mistake: anything that is not a
// region name at all is refused here, at the point where it was written.
var regionShape = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-[0-9]+$`)

// validateRegion rejects what is definitely not a usable region name.
func validateRegion(region string) error {
	if region == "" {
		return fmt.Errorf("oci: no region given")
	}
	// Case is checked before anything else, because the alternative -- folding
	// it -- would give one region two spellings. That is the same reason short
	// names are refused below, and it is not merely tidiness: cmd/mv.go's
	// guard against a recursive move into its own descendant compares the
	// bucket strings as written, so every extra spelling of one bucket is a
	// path around it (TODO.md item 25). One region, one spelling, keeps that
	// guard exactly as sound as it was.
	if lower := strings.ToLower(region); lower != region {
		return fmt.Errorf("oci: region names are lower case; write %q as %q", region, lower)
	}
	// StringToRegion expands a short name to its canonical form, so a name
	// that comes back changed was a short name -- the only inputs it rewrites.
	// This makes no network call: the SDK's instance-metadata lookup is opt-in
	// through EnableInstanceMetadataServiceLookup, which gsg never calls, so
	// an unrecognised name falls through as itself after a map miss.
	if canonical := string(ocicommon.StringToRegion(region)); canonical != region {
		return fmt.Errorf("oci: %q is a short region name; use the full form %q", region, canonical)
	}
	if !regionShape.MatchString(region) {
		return fmt.Errorf("oci: %q is not a region name; use the full form, for example \"ap-singapore-1\"", region)
	}
	return nil
}

// parseBucketSpec reads the authority of an oci:// path.
//
// The accepted forms are "bucket@region" and "bucket@namespace.region". The
// generic parser cannot know that either means anything in particular, so it
// keeps the whole authority in Bucket and leaves the interpretation here --
// this is the only function that knows the encoding.
//
// Every degenerate spelling is an error rather than a default. A missing
// region used to be filled in from ~/.oci/config, and that is precisely what
// this change removes: silently supplying the one component that decides which
// copy of a bucket name is meant is how a path comes to mean different things
// on different machines.
func parseBucketSpec(spec string) (bucketSpec, error) {
	// The last "@" is the separator. A bucket name cannot contain one, but
	// splitting on the last means a surprising name degrades predictably
	// instead of silently swapping the fields.
	at := strings.LastIndex(spec, "@")
	if at < 0 {
		return bucketSpec{}, fmt.Errorf(
			"oci: %q has no region; write it as \"%s@<region>\", for example \"%s@ap-singapore-1\"",
			spec, spec, spec)
	}
	s := bucketSpec{name: spec[:at]}
	if s.name == "" {
		return bucketSpec{}, fmt.Errorf("oci: %q names no bucket", spec)
	}

	qualifier := spec[at+1:]

	// The last dot is the separator, for the same reason the last "@" is: a
	// region name never contains a dot, but a namespace may. Most are
	// alphanumeric, and Oracle's own tooling assumes that -- dedicated
	// endpoints are only offered to tenancies whose namespace is alphanumeric
	// -- but a few older tenancies carry underscores, dashes or periods, and
	// splitting on the first dot would make those namespaces unaddressable.
	if dot := strings.LastIndex(qualifier, "."); dot >= 0 {
		s.namespace, s.region = qualifier[:dot], qualifier[dot+1:]
		if s.namespace == "" {
			return bucketSpec{}, fmt.Errorf(
				"oci: %q has an empty namespace; drop the dot to use the tenancy's own", spec)
		}
	} else {
		s.region = qualifier
	}
	if err := validateRegion(s.region); err != nil {
		return bucketSpec{}, fmt.Errorf("%w (in %q)", err, spec)
	}
	return s, nil
}
