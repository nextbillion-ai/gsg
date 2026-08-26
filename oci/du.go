package oci

import (
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"
)

// DiskUsage reports the size of everything under prefix, as a tree.
func (o *OCI) DiskUsage(bucket, prefix string, recursive bool) ([]system.DiskUsage, error) {
	// A path that names an object is its own answer. Checked first, because
	// listing with that prefix would also match "prefix-other.txt".
	head, err := o.headObject(bucket, prefix)
	if err != nil {
		return nil, err
	}
	if head != nil {
		var size int64
		if head.ContentLength != nil {
			size = *head.ContentLength
		}
		return []system.DiskUsage{{Size: size, Name: prefix}}, nil
	}

	// Sizes come from the listing, so unlike the s3 backend this is one call
	// per page rather than one per object -- the cost TODO item 9 describes.
	objects, _, err := o.walkObjects(bucket, prefix, recursive)
	if err != nil {
		return nil, err
	}

	root := system.NewDUTree(prefix, 0, true)
	for i := range objects {
		s := objects[i]
		if s.Name == nil {
			// Undercounting in silence is the failure mode to avoid here: a
			// total that is quietly short looks exactly like a correct one.
			logger.Info(module, "skipping an entry under oci://%s/%s that has no name", bucket, prefix)
			continue
		}
		var size int64
		if s.Size != nil {
			size = *s.Size
		}
		root.Add(*s.Name, size, prefix)
	}
	return root.ToDiskUsages(), nil
}
