package oci

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/nextbillion-ai/gsg/bar"
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"
	"github.com/nextbillion-ai/gsg/worker"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// defaultDownloadChunkSize is how much one ranged GET asks for when
// --chunk-size says nothing. It matches what gs and s3 use.
const defaultDownloadChunkSize int64 = 16 * 1024 * 1024

// downloadGeometry decides how the object is cut up.
//
// requested is --chunk-size: negative means "no preference", and zero means
// "do not chunk at all", which is how both other backends read it.
func downloadGeometry(size, requested int64) (chunkSize int64, chunks int) {
	chunkSize = requested
	switch {
	case chunkSize < 0:
		chunkSize = defaultDownloadChunkSize
	case chunkSize == 0:
		chunkSize = size
	}
	if chunkSize <= 0 {
		chunkSize = 1
	}
	chunks = int(math.Ceil(float64(size) / float64(chunkSize)))
	if chunks <= 0 {
		// An empty object is still one chunk, of nothing, so no caller has to
		// special-case it.
		chunks = 1
	}
	return chunkSize, chunks
}

// rangeHeader renders a byte range as RFC 7233 asks for it, and says whether a
// range is wanted at all.
//
// length is exact when it is positive, and "to the end of the object" when it
// is negative. Zero asks for nothing at all: an empty object is fetched
// without a range rather than with the "bytes=0--1" that the arithmetic would
// otherwise produce.
//
// The endpoints are *inclusive*, which is the easy thing to get wrong: the s3
// backend asks for `bytes=%d-%d` of startByte and startByte+length, so every
// chunk there fetches one byte more than it needs and the last one asks past
// the end of the object.
func rangeHeader(offset, length int64) (string, bool) {
	if offset < 0 {
		offset = 0
	}
	switch {
	case length > 0:
		return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1), true
	case length < 0:
		if offset == 0 {
			return "", false // the whole object: no range to ask for
		}
		return fmt.Sprintf("bytes=%d-", offset), true
	default:
		return "", false
	}
}

// runChunks runs one job per chunk, bounded, and waits for all of them; see
// worker.RunChunks. A library caller need not pass a pool at all: jam-core
// builds a system.RunContext with a ChunkSize and nothing else, and gets a
// local bound -- its Concurrency, or common.PartConcurrency without one --
// rather than a nil dereference or a serial download.
func runChunks(pool *worker.Pool, chunks, bound int, job func(i int)) {
	worker.RunChunks(pool, chunks, bound, job)
}

// fetchWithRetry runs one chunk again rather than the whole transfer.
//
// This is what chunking buys beyond speed, and neither gs nor s3 has it: a
// failed chunk fails their whole download, which cp and rsync then try again
// from the start, so a blip 79 GiB into an 80 GiB object costs all of it.
//
// The progress bar has to be wound back by hand. A failed attempt has already
// reported whatever it managed to write, and the attempt that replaces it
// reports the same bytes again -- so without this a retried chunk counts twice
// and the bar runs ahead of the transfer.
func fetchWithRetry(pb *bar.ProgressBar, fetch func() (uint32, int64, error)) (uint32, int64, error) {
	var sum uint32
	var written int64
	config := common.DefaultRetryConfig()
	config.Retryable = common.Retryable
	err := common.DoWithRetry(func() error {
		crc, n, ferr := fetch()
		if ferr != nil {
			if pb != nil && n > 0 {
				pb.IncrBy(-n)
			}
			return ferr
		}
		sum, written = crc, n
		return nil
	}, config)
	if err != nil {
		return 0, 0, err
	}
	return sum, written, nil
}

// progressWriter hands back a writer for the bar, or nothing at all when there
// is no bar.
//
// A *bar.ProgressBar that is nil must not be passed as an io.Writer: the
// interface value would not be nil, so the callee writes to it, and IncrBy
// dereferences the nil receiver. Callers without a bar are ordinary -- the
// library API builds a RunContext with no Bars at all -- so this is the common
// path rather than a corner of it.
func progressWriter(pb *bar.ProgressBar) io.Writer {
	if pb == nil {
		return nil
	}
	return pb
}

// Download fetches an object to dstFile.
//
// The object is fetched in parallel ranged chunks rather than as one stream.
// One stream is one connection and one congestion window, so its throughput is
// whatever that single connection manages however much bandwidth the host has
// -- #68 measured the same ceiling from the other direction on gs.
//
// Every chunk is pinned to the version the first HEAD saw, with If-Match.
// Independent ranged GETs by name can land either side of an overwrite, and
// the pieces would assemble into a file that never existed -- silently, when
// -v is off. Verification is against the checksum that same HEAD returned
// rather than a fresh lookup, for the same reason: an overwrite landing after
// the last chunk would otherwise have a correctly assembled copy of the pinned
// version compared against the replacement's checksum and reported corrupt.
//
// The body streams to a temporary file that is renamed into place once it is
// complete, so an interrupted transfer cannot leave a half-written file
// looking like the real one -- a later rsync would see the wrong size and copy
// it again, but anything reading the path directly would not.
func (o *OCI) Download(bucket, prefix, dstFile string, forceChecksum bool, ctx system.RunContext) error {
	ref, err := o.resolve(bucket)
	if err != nil {
		return err
	}

	// Size, mtime, etag and checksum up front: the size gives the progress bar
	// something to count against and decides the chunks, the mtime has to be
	// applied after the rename, and the other two are what every chunk is
	// pinned to and what the result is checked against.
	head, err := o.headObject(bucket, prefix)
	if err != nil {
		return err
	}
	if head == nil {
		return common.Caused(fmt.Sprintf("oci: no object at oci://%s/%s", ref.name, prefix), common.ErrNoSuchObject)
	}
	var size int64
	if head.ContentLength != nil {
		size = *head.ContentLength
	}
	var pin *string
	if head.ETag != nil && *head.ETag != "" {
		pin = head.ETag
	}
	want, stored := crc32cOf(head.OpcContentCrc32c)
	unreadable := !stored && !noCRC32CStored(head.OpcContentCrc32c)

	// The destination's parent may not exist yet: a recursive copy walks
	// objects, not directories, so "a/b/c.txt" can be the first thing that
	// needs "a/b". Without this the open below fails with "no such file or
	// directory" for every object below the top level.
	folder, _ := common.ParseFile(dstFile)
	common.CreateFolder(folder)

	dstFileTemp := common.GetTempFile(dstFile)
	if err = common.CreateFile(dstFileTemp, size); err != nil {
		logger.Info(module, "cannot create %s: %s", dstFileTemp, err)
		_ = os.Remove(dstFileTemp)
		return err
	}

	chunkSize, chunks := downloadGeometry(size, ctx.ChunkSize)
	logger.Debug(module, "Downloading [%s] with %d chunk(s), chunk size: %d bytes, total size: %d bytes",
		prefix, chunks, chunkSize, size)

	var pb *bar.ProgressBar
	if ctx.Bars != nil {
		pb = ctx.Bars.New(size, fmt.Sprintf("Downloading [%s]:", prefix))
	}

	sums := make([]uint32, chunks)
	lens := make([]int64, chunks)
	errs := make([]error, chunks)
	runChunks(ctx.Pool, chunks, ctx.Concurrency, func(i int) {
		// A panic in one chunk would otherwise take the process down and leave
		// the temporary file behind; as an error it goes out the same way a
		// failed request does.
		defer func() {
			if r := recover(); r != nil {
				errs[i] = common.PanicError(fmt.Sprintf("oci: panic downloading chunk %d of oci://%s/%s", i+1, ref.name, prefix), r)
			}
		}()
		off := int64(i) * chunkSize
		length := chunkSize
		if off+length > size {
			length = size - off
		}
		sums[i], lens[i], errs[i] = fetchWithRetry(pb, func() (uint32, int64, error) {
			return o.fetchChunk(ref, prefix, pin, dstFileTemp, off, length, pb, ctx.GentleIO)
		})
	})
	for _, e := range errs {
		if e != nil {
			logger.Info(module, "cannot fetch oci://%s/%s: %s", ref.name, prefix, e)
			_ = os.Remove(dstFileTemp)
			return e
		}
	}

	// Every chunk summed what it wrote, so folding those is free. Reading the
	// file back instead is a second pass as large as the file, and in gentle
	// mode one straight off the disk: those pages were dropped on purpose.
	// Settled before the rename, so a download that does not match leaves
	// whatever was at dstFile as it was.
	local, total := common.FoldCRC32C(sums, lens)
	keep, cache, err := settleDownload(forceChecksum, dstFile, ref.name, prefix, local, total, size, want, stored, unreadable)
	if !keep {
		_ = os.Remove(dstFileTemp)
		return err
	}

	if err = os.Rename(dstFileTemp, dstFile); err != nil {
		logger.Info(module, "cannot move %s into place: %s", dstFileTemp, err)
		_ = os.Remove(dstFileTemp)
		return err
	}
	if head.LastModified != nil {
		common.SetFileModificationTime(dstFile, head.LastModified.Time)
	}
	if cache {
		common.StoreFileCRC32C(dstFile, local)
	}
	return nil
}

// fetchChunk fetches one range into dstFileTemp at its own offset, and returns
// the CRC32C of what it wrote and how many bytes that was.
//
// The byte count is checked against what was asked for. A service that ignored
// the range would answer every chunk with the whole object, and each would
// then write the whole object at its own offset -- so the check is what keeps
// that from assembling silently.
func (o *OCI) fetchChunk(ref bucketRef, prefix string, pin *string, dstFileTemp string, offset, length int64, pb *bar.ProgressBar, gentle bool) (uint32, int64, error) {
	req := objectstorage.GetObjectRequest{
		NamespaceName: &ref.ns, BucketName: &ref.name, ObjectName: &prefix,
		IfMatch: pin,
	}
	if header, ranged := rangeHeader(offset, length); ranged {
		req.Range = &header
	}
	r, err := ref.c.GetObject(context.Background(), req)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		if r.Content != nil {
			_ = r.Content.Close()
		}
	}()
	if r.Content == nil {
		return 0, 0, fmt.Errorf("oci: oci://%s/%s returned no body", ref.name, prefix)
	}

	fl, err := os.OpenFile(dstFileTemp, os.O_WRONLY, 0o644)
	if err != nil {
		return 0, 0, fmt.Errorf("oci: cannot open %s: %w", dstFileTemp, err)
	}
	defer func() { _ = fl.Close() }()
	if _, err = fl.Seek(offset, io.SeekStart); err != nil {
		return 0, 0, fmt.Errorf("oci: cannot seek %s to %d: %w", dstFileTemp, offset, err)
	}

	var sum uint32
	var written int64
	if gentle {
		sum, written, err = common.GentleWrite(fl, offset, length, r.Content, progressWriter(pb))
	} else {
		bufWriter := bufio.NewWriterSize(fl, 4*1024*1024)
		chunkSum := common.NewChunkCRC32C(length)
		var w io.Writer = io.MultiWriter(bufWriter, chunkSum)
		if pb != nil {
			w = io.MultiWriter(bufWriter, pb, chunkSum)
		}
		written, err = io.Copy(w, r.Content)
		if err == nil {
			// Flush explicitly and check it. A deferred flush discards its
			// error, which is the difference between a truncated file and a
			// reported failure.
			err = bufWriter.Flush()
		}
		sum, _ = chunkSum.Sum()
	}
	if err != nil {
		return 0, written, err
	}
	if written != length {
		return 0, written, fmt.Errorf("oci: oci://%s/%s gave %d bytes for the %d asked for at offset %d",
			ref.name, prefix, written, length, offset)
	}
	return sum, written, nil
}

// settleDownload reports on the transfer before its temp file is renamed, fails
// it when asked to, and says whether the file goes into place and whether its
// CRC32C, matching the object's, is worth caching so the next rsync compares
// without reading the file.
//
// A file that does not match never goes into place. Under -v that is the
// download's error; without -v it is logged and the download returns nil, but
// whatever was at dstFile -- the previous copy, or nothing -- stays, rather
// than bytes known to be wrong stamped with the object's time, which a later
// rsync without -v trusts.
//
// local and total are what the chunks summed while writing. want is what the
// HEAD that started the download reported, and stored says whether it reported
// one at all: OCI records a CRC32C only when the uploader asked for one, and a
// missing checksum is not a checksum of zero. unreadable says the HEAD reported
// one that cannot be decoded: under -v that fails rather than passing as if
// there were nothing to compare, and without -v the file goes into place
// uncached, as for a missing one.
func settleDownload(forceChecksum bool, dstFile, bucket, prefix string, local uint32, total, size int64, want uint32, stored, unreadable bool) (keep, cache bool, err error) {
	if !stored && unreadable && forceChecksum {
		log := fmt.Sprintf("CRC32C checking failed for bucket[%s] prefix[%s]: the stored CRC32C cannot be read", bucket, prefix)
		logger.Info(module, log)
		return false, false, fmt.Errorf("%s", log)
	}
	if !stored {
		// Nothing to check against. Saying so is the honest outcome: failing
		// would reject every object written without a CRC32C, and passing in
		// silence is what makes a -v flag worthless. Said only when the caller
		// asked, since every download measures itself and has no reason to
		// report on a check nobody wanted.
		if forceChecksum {
			logger.Info(module, "CRC32C checking skipped for bucket[%s] prefix[%s]: no CRC32C stored", bucket, prefix)
		}
		return true, false, nil
	}
	if forceChecksum {
		logger.Info(module, "CRC32C checking of local[%s] and bucket[%s] prefix[%s] are [%d] with [%d].",
			dstFile, bucket, prefix, local, want)
	}
	if total != size || local != want {
		log := fmt.Sprintf("CRC32C checking failed of local[%s] and bucket[%s] prefix[%s]: %d bytes summing to [%d], object has %d bytes and [%d].",
			dstFile, bucket, prefix, total, local, size, want)
		if !forceChecksum {
			// Every download sums itself, because the sums are free. Reporting
			// a disagreement is worth doing; failing a transfer the caller did
			// not ask to have verified is not.
			logger.Info(module, "%s %s is left as it was.", log, dstFile)
			return false, false, nil
		}
		logger.Info(module, log)
		return false, false, fmt.Errorf("%s", log)
	}
	if forceChecksum {
		logger.Info(module, "CRC32C checking success of local[%s] and bucket[%s] prefix[%s].", dstFile, bucket, prefix)
	}
	return true, true, nil
}
