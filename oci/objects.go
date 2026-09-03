package oci

import (
	"context"
	"fmt"
	"strings"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// listFields is what ListObjects is asked to return for each entry.
//
// Requesting these means size and mtime arrive with the listing and need no
// per-object call, which is the cost TODO item 9 describes on the s3 side.
// CRC32C is not available here at any price -- OCI only returns it from a
// HEAD -- so it is fetched lazily, if at all, via Attrs.CalcCRC32C.
const listFields = "name,size,timeCreated,timeModified,md5,etag"

// maxListPages bounds the pagination loop.
//
// A loop that trusts the server to eventually stop can spin forever if a page
// ever comes back without advancing the cursor; #45 fixed exactly that on the
// s3 side. At 1000 objects per page this still allows ten million objects,
// which is far past any prefix gsg is asked about, so the bound can only be
// reached by a bug or a misbehaving service.
const maxListPages = 10000

// walkObjects returns every object under prefix, and -- when not recursive --
// the immediate sub-directories alongside them.
//
// Recursion is expressed to the service rather than filtered here: with no
// delimiter every key under the prefix comes back, and with "/" the service
// collapses anything deeper into Prefixes.
func (o *OCI) walkObjects(spec, prefix string, recursive bool) ([]objectstorage.ObjectSummary, []string, error) {
	ref, err := o.resolve(spec)
	if err != nil {
		return nil, nil, err
	}
	c, ns, bucket := ref.c, ref.ns, ref.name

	// "some/dir" and "some/dir/" are different requests. Asked for the former
	// with a delimiter, the service answers with one common prefix -- the
	// directory itself -- rather than its contents, so `gsg ls oci://b/dir`
	// would print just the directory it was asked about. The other two
	// backends resolve this the same way: if the path is not an object, treat
	// it as a directory and give the service the trailing slash it needs.
	//
	// Only when a delimiter is in play. A recursive listing wants every key
	// under the prefix and a slash would exclude an object named exactly
	// "some/dir".
	if !recursive && prefix != "" && !strings.HasSuffix(prefix, "/") {
		r, herr := o.headObject(spec, prefix)
		if herr != nil {
			return nil, nil, herr
		}
		if r == nil {
			prefix = common.SetPrefixAsDirectory(prefix)
		}
	}

	var (
		objects  []objectstorage.ObjectSummary
		prefixes []string
		start    *string
		fields   = listFields
	)
	delimiter := ""
	if !recursive {
		delimiter = "/"
	}

	for page := 0; ; page++ {
		if page >= maxListPages {
			return nil, nil, fmt.Errorf(
				"oci: listing oci://%s/%s did not finish after %d pages", bucket, prefix, maxListPages)
		}
		req := objectstorage.ListObjectsRequest{
			NamespaceName: &ns,
			BucketName:    &bucket,
			Fields:        &fields,
			Start:         start,
		}
		if prefix != "" {
			p := prefix
			req.Prefix = &p
		}
		if delimiter != "" {
			d := delimiter
			req.Delimiter = &d
		}
		r, lerr := c.ListObjects(context.Background(), req)
		if lerr != nil {
			logger.Info(module, "listing oci://%s/%s failed: %s", bucket, prefix, lerr)
			return nil, nil, lerr
		}
		objects = append(objects, r.Objects...)
		prefixes = append(prefixes, r.Prefixes...)

		// NextStartWith is the only signal that there is more. An empty page is
		// not the end condition: with a delimiter a page can carry prefixes and
		// no objects and still continue, which is the shape that truncated an
		// s3 listing at 1000 of 1005 entries before #45.
		if r.NextStartWith == nil || *r.NextStartWith == "" {
			break
		}
		if start != nil && *start == *r.NextStartWith {
			return nil, nil, fmt.Errorf(
				"oci: listing oci://%s/%s stopped advancing at %q", bucket, prefix, *start)
		}
		start = r.NextStartWith
	}
	return objects, dedupePrefixes(prefixes), nil
}

// dedupePrefixes removes repeats while keeping order.
//
// Paging a delimited listing can report the same common prefix on more than
// one page, and a duplicate turns into a duplicated row in ls and a
// double-counted directory in du.
func dedupePrefixes(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, p := range in {
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

// anyEntryUnder reports whether at least one object or sub-directory exists
// under prefix.
//
// The question is existence, not contents, so it asks for a single entry. The
// obvious implementation -- list everything and look at the count -- costs a
// page per thousand objects to answer a yes/no: measured at 237ms for a
// 1005-object prefix, and a prefix holding a million would spend minutes on
// it. The s3 backend does exactly that today; the gs one avoids the worst of
// it by listing non-recursively, which is still a whole page.
//
// A delimiter is used for the same reason: without one, a prefix with a
// million keys underneath would have the service walk them all.
// self, when non-empty, names the entry that is the caller's own prefix rather
// than something beneath it, and is not counted.
func (o *OCI) anyEntryUnder(spec, prefix, self string) (bool, error) {
	ref, err := o.resolve(spec)
	if err != nil {
		return false, err
	}
	c, ns, bucket := ref.c, ref.ns, ref.name
	// Two, not one. An object named exactly self is the zero-byte marker a
	// console's "create folder" writes, and it is the caller's own path rather
	// than something beneath it, so it is not counted -- gs and s3 draw the
	// same line, and gsutil hands such an object back rather than calling it a
	// directory. It also sorts before everything under it, so a limit of one
	// would return only the marker and make a directory carrying one look
	// empty. Measured: listing "phboth/" at limit 1 returns just the marker,
	// at limit 2 the marker and its child.
	limit := 2
	delimiter := "/"
	req := objectstorage.ListObjectsRequest{
		NamespaceName: &ns,
		BucketName:    &bucket,
		Limit:         &limit,
		Delimiter:     &delimiter,
	}
	if prefix != "" {
		p := prefix
		req.Prefix = &p
	}
	r, lerr := c.ListObjects(context.Background(), req)
	if lerr != nil {
		logger.Info(module, "listing oci://%s/%s failed: %s", bucket, prefix, lerr)
		return false, lerr
	}
	for _, o := range r.Objects {
		if o.Name != nil && *o.Name != self {
			return true, nil
		}
	}
	for _, sub := range r.Prefixes {
		if sub != self {
			return true, nil
		}
	}
	return false, nil
}
