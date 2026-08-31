package oci

import (
	"bufio"
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

// Download fetches an object to dstFile.
//
// The body streams to a temporary file that is renamed into place once it is
// complete, so an interrupted transfer cannot leave a half-written file
// looking like the real one -- a later rsync would see the wrong size and
// copy it again, but anything reading the path directly would not.
func (o *OCI) Download(bucket, prefix, dstFile string, forceChecksum bool, ctx system.RunContext) error {
	c, ns, name, err := o.resolve(bucket)
	if err != nil {
		return err
	}

	// Size and mtime up front: the size gives the progress bar something to
	// count against, and the mtime has to be applied after the rename.
	head, err := o.headObject(bucket, prefix)
	if err != nil {
		return err
	}
	if head == nil {
		return fmt.Errorf("oci: no object at oci://%s/%s", name, prefix)
	}
	var size int64
	if head.ContentLength != nil {
		size = *head.ContentLength
	}

	r, err := c.GetObject(context.Background(), objectstorage.GetObjectRequest{
		NamespaceName: &ns, BucketName: &name, ObjectName: &prefix,
	})
	if err != nil {
		logger.Info(module, "cannot fetch oci://%s/%s: %s", name, prefix, err)
		return err
	}
	defer func() {
		if r.Content != nil {
			_ = r.Content.Close()
		}
	}()
	if r.Content == nil {
		return fmt.Errorf("oci: oci://%s/%s returned no body", name, prefix)
	}

	// The destination's parent may not exist yet: a recursive copy walks
	// objects, not directories, so "a/b/c.txt" can be the first thing that
	// needs "a/b". Without this the open below fails with "no such file or
	// directory" for every object below the top level.
	folder, _ := common.ParseFile(dstFile)
	common.CreateFolder(folder)

	dstFileTemp := common.GetTempFile(dstFile)
	common.CreateFile(dstFileTemp, size)
	fl, err := os.OpenFile(dstFileTemp, os.O_WRONLY, 0o644)
	if err != nil {
		logger.Info(module, "cannot open %s: %s", dstFileTemp, err)
		return err
	}

	var pb *bar.ProgressBar
	if ctx.Bars != nil {
		pb = ctx.Bars.New(size, fmt.Sprintf("Downloading [%s]:", prefix))
	}

	// The copy and its error handling are in a closure so the file is closed
	// before the rename, on every path. Renaming a file that still has an open
	// writer is how a short write ends up in the destination.
	copyErr := func() error {
		defer func() { _ = fl.Close() }()
		bufWriter := bufio.NewWriterSize(fl, 4*1024*1024)
		var w io.Writer = bufWriter
		if pb != nil {
			w = io.MultiWriter(bufWriter, pb)
		}
		if _, we := io.Copy(w, r.Content); we != nil {
			return we
		}
		// Flush explicitly and check it. A deferred flush discards its error,
		// which is the difference between a truncated file and a reported
		// failure.
		return bufWriter.Flush()
	}()
	if copyErr != nil {
		logger.Info(module, "cannot write %s: %s", dstFileTemp, copyErr)
		_ = os.Remove(dstFileTemp)
		return copyErr
	}

	if err = os.Rename(dstFileTemp, dstFile); err != nil {
		logger.Info(module, "cannot move %s into place: %s", dstFileTemp, err)
		_ = os.Remove(dstFileTemp)
		return err
	}
	if head.LastModified != nil {
		common.SetFileModificationTime(dstFile, head.LastModified.Time)
	}
	return o.MustEqualCRC32C(forceChecksum, dstFile, bucket, prefix)
}

// crc32cOfReader hashes everything f holds and rewinds it.
//
// The point is that the checksum and the body describe the same bytes: this
// reads the very handle the upload will read, so nothing can be substituted
// underneath it. Reading the cached checksum for the path would be cheaper and
// occasionally wrong, and being wrong costs the whole upload.
func crc32cOfReader(f *os.File) (crc uint32, size int64, err error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	// The byte count comes back with the checksum because both have to
	// describe the same bytes. Taking the length from a stat of the path
	// instead would let ContentLength describe one file while the body and
	// the checksum describe another, if the path is replaced in between --
	// the mismatch this function exists to rule out.
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, 0, fmt.Errorf("oci: cannot read %s to checksum it: %w", f.Name(), err)
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return 0, 0, fmt.Errorf("oci: cannot rewind %s after checksumming it: %w", f.Name(), err)
	}
	return h.Sum32(), n, nil
}

// Upload stores srcFile as an object.
func (o *OCI) Upload(srcFile, bucket, object string, ctx system.RunContext) error {
	c, ns, name, err := o.resolve(bucket)
	if err != nil {
		return err
	}
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
	crc, size, err := crc32cOfReader(f)
	if err != nil {
		return err
	}
	localCRC := crc32cToBase64(crc)

	// The progress bar wraps the handle only now, after the checksum pass has
	// rewound it: attaching it earlier would have counted the file twice.
	var body io.Reader = f
	if ctx.Bars != nil {
		pb := ctx.Bars.New(size, fmt.Sprintf("Uploading [%s]:", object))
		body = io.TeeReader(f, pb)
	}
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
