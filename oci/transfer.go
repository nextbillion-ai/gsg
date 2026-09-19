package oci

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/nextbillion-ai/gsg/bar"
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// crc32cOfReader hashes the bytes the upload is about to send.
//
// The point is that the checksum and the body describe the same bytes: this
// reads the very handle the upload will read, so nothing can be substituted
// underneath it. Reading the cached checksum for the path would be cheaper and
// occasionally wrong, and being wrong costs the whole upload.
//
// Both read the same section of the same handle, so the length cannot describe
// one file while the checksum describes another -- and a file that shrank in
// between gives a short read here, which is an error rather than a checksum of
// less than was promised.
//
// Under gentle I/O this is the read that pauses. It is the one that goes to
// the disk; the body that follows reads the pages it just filled, and drops
// them.
func crc32cOfReader(f *os.File, size int64, gentle bool) (crc uint32, n int64, err error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	section := common.NewGentleSection(f, 0, size, common.Gentle{Pause: gentle}, nil)
	common.FadviseSequentialRead(f, common.Gentle{Pause: gentle})
	read, err := io.Copy(h, section)
	if err != nil {
		return 0, 0, fmt.Errorf("oci: cannot read %s to checksum it: %w", f.Name(), err)
	}
	if read != size {
		return 0, 0, fmt.Errorf("oci: %s is %d bytes, not the %d it measured: it was truncated before its upload started", f.Name(), read, size)
	}
	return h.Sum32(), read, nil
}

// Upload stores srcFile as an object.
func (o *OCI) Upload(srcFile, bucket, object string, ctx system.RunContext) error {
	ref, err := o.resolve(bucket)
	if err != nil {
		return err
	}
	c, ns, name := ref.c, ref.ns, ref.name
	f, err := os.Open(srcFile)
	if err != nil {
		logger.Info(module, "cannot read %s: %s", srcFile, err)
		return err
	}
	defer func() { _ = f.Close() }()

	logger.Info(module, "uploading %s to %s/%s", srcFile, name, object)

	// Record a CRC32C, and send our own so the upload is checked on arrival.
	//
	// Two separate things. Without opc-checksum-algorithm, OCI stores only an
	// MD5, and every comparison gsg makes is CRC32C -- so the object would come
	// back with no comparable checksum, rsync would copy it again on every run,
	// and -v would have nothing to verify against. That is the fix #47 made for
	// s3. Unlike s3 the checksum stays whole-object even for a multipart
	// upload, measured: a 20MB object stored in four parts reports the same
	// CRC32C as the whole file, while its MD5 is the composite-of-parts kind.
	//
	// But the algorithm header alone only asks the service to compute a
	// checksum of whatever reached it. Unlike the aws sdk, this one never
	// computes a checksum itself -- the field is documented as "computed by
	// the server" -- so an upload corrupted in transit would be stored with a
	// checksum of the corrupted bytes, and a later -v would compare the two
	// and pass. Sending opc-content-crc32c makes the service compare against
	// what we measured locally and reject the object with HTTP 400 if they
	// differ, so corruption fails the upload instead of being preserved.
	//
	// The checksum comes from the handle the body will be read from, not from
	// the cache keyed on path and mtime. The cache is only as good as the
	// assumption that content and modification time move together, and where
	// that is wrong the checksum describes different bytes than the body does
	// -- which the service notices, so the price of a stale entry is a whole
	// upload spent to be told it was stale. On the gs side that trade was
	// measured at 111ms of hashing against a 4s upload of the same 190MB file.
	fi, err := f.Stat()
	if err != nil {
		logger.Info(module, "cannot measure %s: %s", srcFile, err)
		return err
	}
	fileSize := fi.Size()

	// Above the threshold the object goes up in parts. Nothing forces this on
	// oci -- a single PutObject stored a 40 GiB object fine, and so did
	// Oracle's own CLI with --no-multipart -- but measured on a 2 GiB object,
	// one request managed 36 MB/s against 47-55 MB/s with parts in flight
	// together, and a part that fails costs one part rather than the whole
	// transfer.
	if fileSize > ociMultipartThreshold {
		partSize, parts := common.PartGeometry(fileSize, ctx.ChunkSize, ociMinPartSize, ociMaxPartSize)
		var mpb *bar.ProgressBar
		if ctx.Bars != nil {
			mpb = ctx.Bars.New(fileSize, fmt.Sprintf("Uploading [%s]:", object))
		}
		return o.uploadMultipart(f, fi, bucket, object, partSize, parts, mpb, ctx.GentleIO)
	}

	crc, size, err := crc32cOfReader(f, fileSize, ctx.GentleIO)
	if err != nil {
		return err
	}
	localCRC := crc32cToBase64(crc)

	// The body counts its own bytes, and under --gentle-io drops each window
	// as it goes. It replaces an io.TeeReader, which was not only unpaced: the
	// SDK reflects into the body looking for an io.Seeker so it can rewind and
	// retry, and a TeeReader is not one -- so attaching a progress bar, which
	// is what the CLI always does, turned the SDK's own retry off. A section
	// reader seeks.
	//
	// It wraps the handle only now, after the checksum pass has rewound it:
	// attaching it earlier would have counted the file twice.
	var pb *bar.ProgressBar
	if ctx.Bars != nil {
		pb = ctx.Bars.New(size, fmt.Sprintf("Uploading [%s]:", object))
	}
	body := common.NewGentleSection(f, 0, size, common.Gentle{Drop: ctx.GentleIO}, progressWriter(pb))
	if _, err = c.PutObject(context.Background(), objectstorage.PutObjectRequest{
		NamespaceName:        &ns,
		BucketName:           &name,
		ObjectName:           &object,
		ContentLength:        &size,
		PutObjectBody:        io.NopCloser(body),
		OpcChecksumAlgorithm: objectstorage.PutObjectOpcChecksumAlgorithmCrc32c,
		OpcContentCrc32c:     &localCRC,
	}); err != nil {
		logger.Info(module, "cannot upload %s to oci://%s/%s: %s", srcFile, name, object, err)
		return err
	}
	return nil
}
