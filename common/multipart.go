package common

// Part sizing for multipart uploads.
//
// Both s3 and oci cap a multipart upload at 10000 parts, and both require every
// part except the last to be at least a few MiB. A fixed part size therefore
// cannot work on its own: 128 MiB parts run out at 1.2 TiB, so the size has to
// grow for objects past that rather than the upload failing.
const (
	// DefaultPartSize is what an upload uses when nothing else is asked for.
	//
	// Measured against oci with a 2 GiB file: 64, 128 and 256 MiB parts at
	// concurrency 4, 8 and 16 all landed between 39s and 46s, against 56s for
	// a single PutObject. The whole 3x3 matrix was within noise of itself, so
	// this value is not load-bearing -- what matters is that there is more
	// than one part in flight at all.
	DefaultPartSize int64 = 128 * 1024 * 1024

	// MaxParts is the limit both services impose.
	MaxParts int64 = 10000
)

// PartGeometry returns the part size and part count to use for an object.
//
// requested is the caller's preference -- the --chunk-size flag -- where a
// negative value means "no preference". It is clamped into what the service
// accepts, and then doubled until the object fits within MaxParts, so a large
// enough object silently gets larger parts instead of being rejected.
//
// maxPart matters because --chunk-size is a user-supplied number: without the
// clamp, asking for parts larger than the service allows would turn a
// perfectly valid upload into a rejected one.
func PartGeometry(size, requested, minPart, maxPart int64) (partSize int64, parts int64) {
	partSize = requested
	if partSize <= 0 {
		partSize = DefaultPartSize
	}
	if partSize < minPart {
		partSize = minPart
	}
	if maxPart > 0 && partSize > maxPart {
		partSize = maxPart
	}
	// Grow rather than fail. Doubling keeps this to at most a few iterations
	// even for the largest objects either service accepts.
	for (size+partSize-1)/partSize > MaxParts {
		partSize *= 2
		if maxPart > 0 && partSize > maxPart {
			// Nothing more can be done here: an object needing more than
			// MaxParts at the largest part the service takes cannot be stored
			// this way at all. Leave the geometry at the cap and let the
			// service say so, rather than silently sending something invalid.
			partSize = maxPart
			break
		}
	}
	parts = (size + partSize - 1) / partSize
	if parts < 1 {
		// A zero-byte object still has one (empty) part, so callers never have
		// to special-case an empty range.
		parts = 1
	}
	return partSize, parts
}

// PartConcurrency bounds how many parts of one object are in flight.
//
// Deliberately not the shared worker pool. A recursive copy already runs one
// pool worker per file, and drawing part workers from the same pool would let
// a big enough tree deadlock waiting on itself. A small local bound also keeps
// a recursive upload of many large files from multiplying file-level
// concurrency by part-level concurrency.
func PartConcurrency(parts int64) int {
	const maxInFlight = 8
	if parts < int64(maxInFlight) {
		return int(parts)
	}
	return maxInFlight
}
