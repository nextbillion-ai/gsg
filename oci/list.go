package oci

import (
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"
)

// List returns the objects under prefix, and -- when not recursive -- the
// immediate sub-directories alongside them.
func (o *OCI) List(bucket, prefix string, recursive bool) ([]*system.FileObject, error) {
	objects, prefixes, err := o.walkObjects(bucket, prefix, recursive)
	if err != nil {
		return nil, err
	}

	// No de-duplication between objects and prefixes here, deliberately.
	// A zero-length "folder/" marker and the common prefix for the same path
	// would be one directory reported twice -- the shape #35 fixed on the gs
	// side -- but measured against the service that pairing never occurs:
	//
	//	list "markertest/"      -> objects: none,               prefixes: [markertest/sub/]
	//	list "markertest/sub/"  -> objects: [markertest/sub/, .../c.txt],
	//	                           prefixes: [markertest/sub/deep/]
	//
	// A delimited listing absorbs the marker into the prefix, and where the
	// marker is returned it is because the caller asked for that directory, so
	// its name is never also in Prefixes. Guarding against it would be code no
	// test can reach.

	fos := make([]*system.FileObject, 0, len(objects)+len(prefixes))
	for i := range objects {
		s := objects[i]
		if s.Name == nil {
			// The service documents name as mandatory. If one ever arrives
			// without it there is nothing to address the object by, and a
			// FileObject with an empty prefix would be dereferenced by callers
			// as though it named something.
			logger.Info(module, "skipping an entry of oci://%s/%s that has no name", bucket, prefix)
			continue
		}
		fo := &system.FileObject{
			System: o,
			Bucket: bucket,
			Prefix: *s.Name,
			Remote: true,
		}
		// SetAttributes decides object vs directory from the trailing slash,
		// so a marker comes out as a directory rather than a zero-byte file.
		// A recursive listing does surface markers as rows; that matches the
		// s3 backend, which lists them the same way, and changing it would be
		// a decision about every backend rather than about this one.
		fo.SetAttributes(o.summaryToAttrs(bucket, s))
		fos = append(fos, fo)
	}

	for _, p := range prefixes {
		fo := &system.FileObject{
			System: o,
			Bucket: bucket,
			Prefix: p,
			Remote: true,
		}
		// A directory has no size or checksum of its own. Leaving CalcCRC32C
		// unset matters: Same calls it whenever it is set, so a directory
		// carrying one would make every comparison head an object that is not
		// there.
		fo.SetAttributes(&system.Attrs{})
		fos = append(fos, fo)
	}
	return fos, nil
}
