package gcs

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nextbillion-ai/gsg/bar"
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
)

const (
	googleApplicationCredentialsEnv = "GOOGLE_APPLICATION_CREDENTIALS"
	module                          = "GCS"
	// lockCacheSize is the byte length of a cached lock generation.
	lockCacheSize = 8
	// lockCachePerm keeps the cache private. os.ModePerm made it world
	// readable and writable, and cross-user unlock cannot work anyway: the
	// generation is specific to whoever acquired the lock.
	lockCachePerm = 0600
)

// ConfigPath gets gcp config path from env
func ConfigPath() string {
	return os.Getenv(googleApplicationCredentialsEnv)
}

type GCS struct {
	composeMu sync.Mutex
	composeOK map[string]bool
	// mu guards the lazy client. One GCS is registered for the whole process
	// and every worker goroutine calls Init, so the check-then-set this
	// replaces raced: two goroutines could both find a nil client and both
	// build one, leaving one leaked.
	mu     sync.Mutex
	client *storage.Client
}

func (g *GCS) Scheme() string {
	return "gs"
}

func (g *GCS) toAttrs(attrs *storage.ObjectAttrs) *system.Attrs {
	if attrs == nil {
		return nil
	}
	return &system.Attrs{
		Size:    attrs.Size,
		CRC32:   attrs.CRC32C,
		ModTime: GetFileModificationTime(attrs),
	}
}

func (g *GCS) toFileObject(attrs *storage.ObjectAttrs, bucket string) *system.FileObject {
	if attrs == nil {
		return nil
	}
	name := attrs.Prefix
	if len(name) == 0 {
		name = attrs.Name
	}
	fo := &system.FileObject{
		System: g,
		Bucket: bucket,
		Prefix: name,
		Remote: true,
	}
	fo.SetAttributes(g.toAttrs(attrs))
	return fo
}

// storageClient gets or creates a gcp storage client
func (g *GCS) Init(_ ...string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.client != nil {
		return nil
	}
	path := ConfigPath()
	if path == "" {
		log := fmt.Sprintf("gcs: expected env-var [%s] not found", googleApplicationCredentialsEnv)
		logger.Info(module, log)
		return fmt.Errorf(log)
	}
	var err error
	if _, err = os.Stat(path); err != nil {
		logger.Info(module, "gcs: failed in loading [%s=%s] with error: %s", googleApplicationCredentialsEnv, path, err)
		return err
	}
	// Over HTTP/2 every chunk of a transfer is multiplexed onto one TCP connection, which
	// caps a gsg process at roughly 230 MB/s. HTTP/1.1 with a keep-alive pool spreads the
	// chunks over as many connections as there are workers. GSG_HTTP2=1 keeps HTTP/2.
	var clientOpts []option.ClientOption
	if os.Getenv("GSG_HTTP2") != "1" {
		base := &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			IdleConnTimeout:       90 * time.Second,
			ExpectContinueTimeout: time.Second,
			MaxIdleConns:          1024,
			MaxIdleConnsPerHost:   1024,
			ForceAttemptHTTP2:     false,
			// no h2 in ALPN either, or the server still negotiates HTTP/2
			TLSClientConfig: &tls.Config{NextProtos: []string{"http/1.1"}},
			TLSNextProto:    map[string]func(string, *tls.Conn) http.RoundTripper{},
		}
		rt, terr := htransport.NewTransport(context.Background(), base, option.WithCredentialsFile(path), option.WithScopes(storage.ScopeFullControl))
		if terr != nil {
			logger.Info(module, "get transport failed with %s", terr)
			return terr
		}
		clientOpts = []option.ClientOption{option.WithHTTPClient(&http.Client{Transport: rt})}
	} else {
		clientOpts = []option.ClientOption{option.WithCredentialsFile(path)}
	}
	g.client, err = storage.NewClient(context.Background(), clientOpts...)
	if err != nil {
		logger.Info(module, "get client failed with %s", err)
		return err
	}
	return nil
}

func (g *GCS) GCSAttrs(bucket, prefix string) (*storage.ObjectAttrs, error) {
	var err error
	if err = g.Init(); err != nil {
		return nil, err
	}
	if prefix == "" {
		return nil, nil
	}
	attrs, err := g.client.Bucket(bucket).Object(prefix).Attrs(context.Background())
	if err != nil {
		logger.Debug(module, "failed with gs://%s/%s %s", bucket, prefix, err)
		return nil, nil
	}
	return attrs, nil
}

// GetObjectAttributes gets the attributes of an object
func (g *GCS) Attributes(bucket, prefix string) (*system.Attrs, error) {
	var err error
	var ga *storage.ObjectAttrs
	if ga, err = g.GCSAttrs(bucket, prefix); err != nil {
		return nil, err
	}
	return g.toAttrs(ga), nil
}

func (g *GCS) batchAttrs(bucket, prefix string, recursive bool) ([]*storage.ObjectAttrs, error) {
	var err error
	if err = g.Init(); err != nil {
		return nil, err
	}
	var ok bool
	if ok, err = g.IsObject(bucket, prefix); err != nil {
		return nil, err
	}
	if !ok {
		prefix = common.SetPrefixAsDirectory(prefix)
	}
	res := []*storage.ObjectAttrs{}
	delimiter := "/"
	if recursive {
		delimiter = ""
	}
	it := g.client.Bucket(bucket).Objects(
		context.Background(),
		&storage.Query{
			Delimiter:  delimiter,
			Prefix:     prefix,
			Projection: storage.ProjectionNoACL,
		},
	)
	count := int64(0)
	for {
		count++
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			logger.Info(module, "get objects attributes failed with %s", err)
			return nil, err
		}

		if count%100000 == 0 {
			logger.Info(module, "batchAttrs for bucket[%s] prefix[%s] current count[%d]", bucket, prefix, count)
		}
		if len(attrs.Name) > 0 && common.IsSubPath(attrs.Name, prefix) {
			res = append(res, attrs)
		} else if len(attrs.Prefix) > 0 && common.IsSubPath(attrs.Prefix, prefix) {
			res = append(res, attrs)
		}
	}
	return res, nil
}

// GetObjectsAttributes gets the attributes of all the objects under a prefix
func (g *GCS) BatchAttributes(bucket, prefix string, recursive bool) ([]*system.Attrs, error) {
	var err error
	var gas []*storage.ObjectAttrs
	if gas, err = g.batchAttrs(bucket, prefix, recursive); err != nil {
		return nil, err
	}
	res := []*system.Attrs{}
	for _, attr := range gas {
		res = append(res, g.toAttrs(attr))
	}
	return res, nil
}

// List objects under a prefix
func (g *GCS) List(bucket, prefix string, recursive bool) ([]*system.FileObject, error) {
	var err error
	var gas []*storage.ObjectAttrs
	if gas, err = g.batchAttrs(bucket, prefix, recursive); err != nil {
		return nil, err
	}
	fos := []*system.FileObject{}
	for _, attr := range gas {
		fos = append(fos, g.toFileObject(attr, bucket))
	}
	return fos, nil
}

// GetDiskUsageObjects gets disk usage of objects under a prefix
func (g *GCS) DiskUsage(bucket, prefix string, recursive bool) ([]system.DiskUsage, error) {
	// is object
	var err error
	var obj *storage.ObjectAttrs
	if obj, err = g.GCSAttrs(bucket, prefix); err != nil {
		return nil, err
	}
	if obj != nil {
		return []system.DiskUsage{{Size: obj.Size, Name: obj.Name}}, nil
	}
	root := system.NewDUTree(prefix, 0, true)
	// is directory
	var objs []*storage.ObjectAttrs
	if objs, err = g.batchAttrs(bucket, prefix, recursive); err != nil {
		return nil, err
	}
	for _, obj := range objs {
		name := obj.Name
		if len(name) == 0 {
			name = obj.Prefix
		}
		root.Add(name, obj.Size, prefix)
	}

	return root.ToDiskUsages(), nil
}

func (g *GCS) DeleteObject(bucket, prefix string) error {
	var err error
	if err = g.Init(); err != nil {
		return err
	}
	return g.client.Bucket(bucket).Object(prefix).Delete(context.Background())
}

// DeleteObject deletes an object
func (g *GCS) Delete(bucket, prefix string) error {
	var err error
	if err = g.Init(); err != nil {
		return err
	}
	if err = g.client.Bucket(bucket).Object(prefix).Delete(context.Background()); err != nil {
		logger.Info(module, "delete object failed with %s", err)
		return err
	}
	logger.Info(module, "Removing bucket[%s] prefix[%s]", bucket, prefix)
	return nil
}

// CopyObject copies an object
func (g *GCS) Copy(srcBucket, srcPrefix, dstBucket, dstPrefix string) error {
	var err error
	// check object
	var ga *storage.ObjectAttrs
	if ga, err = g.GCSAttrs(srcBucket, srcPrefix); err != nil {
		return err
	}
	if ga == nil {
		log := fmt.Sprintf("failed with bucket[%s] prefix[%s] not an object", srcBucket, srcPrefix)
		logger.Debug(module, log)
		return fmt.Errorf(log)
	}

	// copy object
	src := g.client.Bucket(srcBucket).Object(srcPrefix)
	dst := g.client.Bucket(dstBucket).Object(dstPrefix)
	if _, err = dst.CopierFrom(src).Run(context.Background()); err != nil {
		logger.Info(module, "copy object failed with %s", err)
		return err
	}
	logger.Info(
		module,
		"Copying from bucket[%s] prefix[%s] to bucket[%s] prefix[%s]",
		srcBucket, srcPrefix, dstBucket, dstPrefix,
	)
	return nil
}

func (g *GCS) GetObjectWriter(bucket, prefix string) (io.WriteCloser, error) {
	var err error
	if err = g.Init(); err != nil {
		return nil, err
	}
	return g.client.Bucket(bucket).Object(prefix).NewWriter(context.Background()), nil
}

func (g *GCS) GetObjectReader(bucket, prefix string) (io.ReadCloser, error) {
	var err error
	if err = g.Init(); err != nil {
		return nil, err
	}
	var rc *storage.Reader
	if rc, err = g.client.Bucket(bucket).Object(prefix).NewReader(context.Background()); err != nil {
		return nil, err
	}
	return rc, nil
}

// DownloadObjectWithWorkerPool downloads a specific byte range of an object to a file.
func (g *GCS) Download(
	bucket, prefix, dstFile string,
	forceChecksum bool,
	ctx system.RunContext,
) error {
	var err error
	var attrs *storage.ObjectAttrs
	// check object
	if attrs, err = g.GCSAttrs(bucket, prefix); err != nil {
		return err
	}
	if attrs == nil {
		log := fmt.Sprintf("failed with bucket[%s] prefix[%s] not an object", bucket, prefix)
		logger.Debug(module, log)
		return fmt.Errorf(log)
	}

	// get chunck size and chunk number
	chunkSize := ctx.ChunkSize
	if chunkSize < 0 {
		chunkSize = int64(googleapi.DefaultUploadChunkSize)
	} else if chunkSize == 0 {
		// chunk size 0 means no chunking, download as single chunk
		chunkSize = attrs.Size
		if chunkSize <= 0 {
			chunkSize = 1
		}
	}
	chunkNumber := int(math.Ceil(float64(attrs.Size) / float64(chunkSize)))
	if chunkNumber <= 0 {
		chunkNumber = 1
	}
	logger.Debug(module, "Downloading [%s] with %d chunk(s), chunk size: %d bytes, total size: %d bytes", prefix, chunkNumber, chunkSize, attrs.Size)

	// paralell copy by range
	//
	// Every chunk reads the generation the lookup above saw. The chunks are
	// separate range reads, and by name each one would read whatever the object
	// is when it opens: an overwrite landing mid-download spliced two versions
	// into a file that never existed, and without -v nothing noticed. Pinned, a
	// chunk opened after the overwrite still reads the old generation where
	// versioning keeps it, and fails where it does not.
	obj := g.client.Bucket(bucket).Object(prefix).Generation(attrs.Generation)
	var pb *bar.ProgressBar
	var wg sync.WaitGroup
	var once sync.Once
	dstFileTemp := common.GetTempFile(dstFile)
	// gentle mode sums each chunk from the file while its pages are still cached
	chunkSums := make([]uint32, chunkNumber)
	chunkLens := make([]int64, chunkNumber)
	for i := 0; i < chunkNumber; i++ {

		// decide offset and length
		i := i
		startByte := int64(i) * chunkSize
		length := chunkSize
		if i == chunkNumber-1 {
			length = attrs.Size - startByte
		}

		wg.Add(1)
		ctx.Pool.AddWithDepth(1,
			func() {
				defer func() {
					wg.Done()
				}()

				// create folder and temp file if not exist
				once.Do(func() {
					pb = ctx.Bars.New(attrs.Size, fmt.Sprintf("Downloading [%s]:", prefix))
					folder, _ := common.ParseFile(dstFile)
					if !common.IsPathExist(folder) {
						common.CreateFolder(folder)
					}
					common.CreateFile(dstFileTemp, attrs.Size)
				})

				// create reader with offset and length of object
				rc, err := obj.NewRangeReader(context.Background(), startByte, length)
				if err != nil {
					logger.Info(module, "download object failed when create reader with %s", err)
					common.Exit()
				}
				defer func() { _ = rc.Close() }()

				// create write with offset and length of file
				fl, _ := os.OpenFile(dstFileTemp, os.O_WRONLY, 0766)
				_, err = fl.Seek(startByte, 0)
				if err != nil {
					logger.Info(module, "download object failed when seek for offset with %s", err)
					common.Exit()
				}

				defer func() { _ = fl.Close() }()

				// If gentle I/O mode, use throttled writer to reduce impact
				if ctx.GentleIO {
					logger.Debug(module, "Using gentle I/O mode with throttled writer for chunk at offset %d", startByte)
					// The chunk is summed from the file as it is written, so the
					// transfer can be settled without reading it back afterwards.
					// That needs a second handle: fl is open for writing and its
					// offset is in use.
					verifier, verr := os.Open(dstFileTemp)
					if verr != nil {
						logger.Info(module, "download object failed when open for verify: %s", verr)
						common.Exit()
					}
					defer func() { _ = verifier.Close() }()
					sum, written, gerr := common.GentleWrite(fl, verifier, startByte, rc, pb)
					if gerr != nil {
						logger.Info(module, "download object failed in gentle mode with %s", gerr)
						common.Exit()
					}
					chunkSums[i], chunkLens[i] = sum, written
				} else {
					// Fast mode: use buffered writer
					bufWriter := bufio.NewWriterSize(fl, 4*1024*1024)
					defer func() { _ = bufWriter.Flush() }()

					if _, err = io.Copy(io.MultiWriter(bufWriter, pb), rc); err != nil {
						logger.Info(module, "download object failed when write to offet with %s", err)
						common.Exit()
					}

					if err = bufWriter.Flush(); err != nil {
						logger.Info(module, "download object failed when flush buffer with %s", err)
						common.Exit()
					}
				}
			},
		)
	}

	// move back the temp file
	wg.Wait()

	// sync temp file to disk before rename
	if tmpFile, err := os.OpenFile(dstFileTemp, os.O_WRONLY, 0766); err == nil {
		_ = tmpFile.Sync()
		_ = tmpFile.Close()
	}

	err = os.Rename(dstFileTemp, dstFile)
	if err != nil {
		logger.Info(module, "download object failed when rename file with %s", err)
		return err
	}
	common.SetFileModificationTime(dstFile, GetFileModificationTime(attrs))
	if ctx.GentleIO {
		return verifyGentleDownload(forceChecksum, dstFile, bucket, prefix, attrs, chunkSums, chunkLens)
	}
	// Settled against the generation the chunks read, not a fresh lookup by
	// name: an overwrite landing after the last chunk would otherwise have a
	// correct copy of the old generation reported as corrupt.
	if !forceChecksum {
		return nil
	}
	return mustMatchCRC32C(dstFile, bucket, prefix, attrs.CRC32C)
}

// verifyGentleDownload settles a gentle download from the sums its chunks took
// while writing. The file is not read again: gentle mode has been asking the
// kernel to drop it from the page cache all along, so that read would come from
// disk, as large as the file, against whatever else is reading that disk.
func verifyGentleDownload(forceChecksum bool, dstFile, bucket, prefix string, attrs *storage.ObjectAttrs, sums []uint32, lens []int64) error {
	crc, total := common.FoldCRC32C(sums, lens)
	if total != attrs.Size || crc != attrs.CRC32C {
		log := fmt.Sprintf("CRC32C checking failed of local[%s] and bucket[%s] prefix[%s]: %d bytes summing to [%d], object has %d bytes and [%d].",
			dstFile, bucket, prefix, total, crc, attrs.Size, attrs.CRC32C)
		logger.Info(module, log)
		if !forceChecksum {
			return nil
		}
		return fmt.Errorf(log)
	}
	common.StoreFileCRC32C(dstFile, crc)
	if forceChecksum {
		logger.Info(module, "CRC32C checking success of local[%s] and bucket[%s] prefix[%s].", dstFile, bucket, prefix)
	}
	return nil
}

// DoAttemptUnlock takes generation as input and returns potential error
func (g *GCS) DoAttemptUnlock(bucket, object string, generation int64) error {
	var err error
	if err = g.Init(); err != nil {
		return err
	}
	o := g.client.Bucket(bucket).Object(object)
	//delete fails means other client has acquired lock
	logger.Debug(module, "DoAttemptUnlock: unlock with generation:%d", generation)
	return o.If(storage.Conditions{GenerationMatch: int64(generation)}).Delete(context.Background())
}

// AttemptUnLock attempts to release a remote lock file
func (g *GCS) AttemptUnLock(bucket, object string) error {
	cacheFileName := common.GenTempFileName(bucket, "/", object)
	generationBytes, e := os.ReadFile(cacheFileName)
	if e != nil {
		logger.Debug(module, "failed to read lock cache: %+v", cacheFileName)
		return nil
	}
	if len(generationBytes) < lockCacheSize {
		// Left short by a run that died mid-write; decoding it panicked with
		// "index out of range". It names no generation, so the remote lock
		// cannot be released and will stand until its TTL expires. Say so
		// rather than reporting a successful unlock.
		//
		// The file is left in place: another process may have just renamed a
		// valid cache over this path, and removing it would discard that. The
		// next successful lock replaces it atomically anyway.
		logger.Info(module, "invalid lock cache [%s] of %d byte(s)", cacheFileName, len(generationBytes))
		return fmt.Errorf("invalid lock cache [%s]: cannot release the lock on gs://%s/%s", cacheFileName, bucket, object)
	}
	generation := binary.LittleEndian.Uint64(generationBytes)
	if e := g.DoAttemptUnlock(bucket, object, int64(generation)); e != nil {
		logger.Debug(module, "unlock error: %+v", e)
		return e
	}
	return nil
}

// DoAttemptLock returns generation and potential error
func (g *GCS) DoAttemptLock(bucket, object string, ttl time.Duration) (int64, error) {
	var err, err1 error
	if err = g.Init(); err != nil {
		return 0, err
	}
	// write lock
	o := g.client.Bucket(bucket).Object(object)
	wc := o.If(storage.Conditions{DoesNotExist: true}).NewWriter(context.Background())
	_, _ = wc.Write([]byte("1"))
	err = wc.Close()
	var attrs *storage.ObjectAttrs
	if attrs, err1 = o.Attrs(context.Background()); err1 != nil {
		return 0, err1
	}
	if err == nil {
		return attrs.Generation, nil
	}
	//logger.Debug("DoAttemptLock expire: %+v, current: %+v, ttl:%+v", attrs.Updated, time.Now(), ttl)
	if attrs.Updated.Add(ttl).Before(time.Now()) {
		//logger.Debug("DoAttemptLock expired. delete and try lock again")
		_ = o.If(storage.Conditions{GenerationMatch: attrs.Generation}).Delete(context.Background())
		//try acquire lock again
		wc = o.If(storage.Conditions{DoesNotExist: true}).NewWriter(context.Background())
		_, _ = wc.Write([]byte("1"))
		if err = wc.Close(); err != nil {
			return 0, err
		}
		if attrs, err1 = o.Attrs(context.Background()); err1 != nil {
			return 0, err1
		}
		return attrs.Generation, nil
	} else {
		//lock acquire failure, quit with error
		return 0, err
	}
	//upon sucessful write, store generation in /tmp
	//logger.Debug("DoAttemptLock lock acquired. updating ttl")
}

// AttemptLock attempts to write a remote lock file
func (g *GCS) AttemptLock(bucket, object string, ttl time.Duration) error {
	generation, e := g.DoAttemptLock(bucket, object, ttl)
	if e != nil {
		logger.Info(module, "attemp lock failed: %s", e)
		return e
	}

	//upon sucessful write, store generation in /tmp
	logger.Debug(module, "AttemptLock: storing generation: %+v", generation)
	cacheFileName := common.GenTempFileName(bucket, "/", object)
	generationBytes := make([]byte, lockCacheSize)
	binary.LittleEndian.PutUint64(generationBytes, uint64(generation))
	if e1 := common.WriteFileAtomic(cacheFileName, generationBytes, lockCachePerm); e1 != nil {
		logger.Info(module, "AttemptLock: cache lock generation failed: %s", e1)
		return e1
	}
	return nil
}

// UploadObject uploads an object from a file
func (g *GCS) Upload(srcFile, bucket, object string, ctx system.RunContext) error {
	var err error
	if err = g.Init(); err != nil {
		return err
	}
	// open source file
	var f *os.File
	if f, err = openSource(srcFile); err != nil {
		logger.Info(module, "upload object failed when open file with %s", err)
		return err
	}
	defer func() { _ = f.Close() }()
	// Everything below is decided from this one stat of the opened file: the
	// bytes come from the handle, and a stat of the path can describe a file
	// that has replaced it since the open.
	before, err := f.Stat()
	if err != nil {
		logger.Info(module, "upload object failed when measuring file with %s", err)
		return err
	}
	size, modTime := before.Size(), before.ModTime()

	// progress bar
	pb := ctx.Bars.New(size, fmt.Sprintf("Uploading [%s]:", srcFile))

	// upload file
	//
	// The writer gets a cancellable context so a failed read can abort the
	// upload. Closing it instead would finalize whatever had been written,
	// publishing a truncated object under a name that now looks complete.
	uploadCtx, abort := context.WithCancel(context.Background())
	defer abort()
	o := g.client.Bucket(bucket).Object(object)
	wc := o.NewWriter(uploadCtx)
	wc.Metadata = map[string]string{
		"goog-reserved-file-mtime": strconv.FormatInt(modTime.UnixNano(), 10),
	}

	// A composite upload sums each part in the copy that sends it and folds the
	// sums (item 27 in TODO.md); only the single stream needs the whole file's
	// value before it starts, and pays a pass of its own for it.
	if ctx.Concurrency > 1 && size > compositeMinSize && g.bucketAllowsCompose(bucket) {
		abort()
		return g.uploadComposite(f, before, modTime, bucket, object, pb, ctx)
	}

	// Send the checksum, so the service checks the body it received against
	// what was measured here and refuses the object if they differ.
	//
	// Without this nothing verified the transfer. GCS still records a CRC32C,
	// but it computes it from whatever arrived -- so an upload corrupted on
	// the way was stored together with a checksum of the corrupted bytes.
	// Everything downstream then agreed: rsync saw a matching object, and -v
	// verified the corruption against itself and passed. s3 has been checked
	// since #47 and oci since #52; this was the last one that was not.
	//
	// Both fields are required and both must be set before the first Write:
	// the library ignores SendCRC32C afterwards, and zero is a valid checksum
	// so it is never transmitted on its own.
	crc, cerr := crc32cToSend(f, srcFile)
	if cerr != nil {
		logger.Info(module, "cannot measure %s, so the upload cannot be verified: %s", srcFile, cerr)
		return cerr
	}
	wc.CRC32C = crc
	wc.SendCRC32C = true
	if _, err = io.Copy(io.MultiWriter(wc, pb), f); err != nil {
		logger.Info(module, "upload object failed when copy file with %s", err)
		abort()
		return err
	}
	// Close finalizes the upload and returns the server's commit errors, which
	// io.Copy above cannot see: it only fills the writer's buffer.
	if err = wc.Close(); err != nil {
		logger.Info(module, "upload object failed when finalizing with %s", err)
		return err
	}
	return nil
}

// A resumable upload is one sequential stream and moves about 60 MB/s however
// large its chunks are. With -m, files above compositeMinSize go as parallel
// parts that GCS composes into the object. The service computes the CRC32C of
// the composition and, given the local file's, refuses the compose when they
// differ, so the destination is only ever replaced by a verified object.
const compositeMinSize = 256 << 20
const compositeMaxParts = 32

// Deadlines for the calls around the parts. The library retries a call it takes
// for idempotent -- any lookup, and a delete pinned to a generation -- for as
// long as its context lives, so on context.Background() a service that keeps
// answering 503 would hold the upload forever.
var partDeleteTimeout = 30 * time.Second
var partLookupTimeout = 30 * time.Second
var bucketAttrsTimeout = 30 * time.Second

// bucketAllowsCompose is false when a retention policy would keep the parts from
// being deleted afterwards. A bucket that cannot be inspected counts as allowed;
// parts left behind are then reported by the upload.
func (g *GCS) bucketAllowsCompose(bucket string) bool {
	g.composeMu.Lock()
	defer g.composeMu.Unlock()
	if ok, seen := g.composeOK[bucket]; seen {
		return ok
	}
	ok := true
	attrsCtx, cancel := context.WithTimeout(context.Background(), bucketAttrsTimeout)
	defer cancel()
	if attrs, err := g.client.Bucket(bucket).Attrs(attrsCtx); err == nil && attrs.RetentionPolicy != nil {
		logger.Info(module, "bucket %s has a retention policy, uploading %s as one stream", bucket, "large objects")
		ok = false
	}
	if g.composeOK == nil {
		g.composeOK = map[string]bool{}
	}
	g.composeOK[bucket] = ok
	return ok
}

func (g *GCS) uploadComposite(f *os.File, before os.FileInfo, modTime time.Time, bucket, object string, pb *bar.ProgressBar, ctx system.RunContext) error {
	size := before.Size()
	parts := int(math.Ceil(float64(size) / float64(compositeMinSize)))
	if parts > compositeMaxParts {
		parts = compositeMaxParts
	}
	partSize := int64(math.Ceil(float64(size) / float64(parts)))
	bkt := g.client.Bucket(bucket)
	// parts are named per upload and referenced by the generation each one was
	// written as, so two uploads of the same object never touch each other's parts
	var uid [8]byte
	if _, err := rand.Read(uid[:]); err != nil {
		return err
	}
	prefix := fmt.Sprintf("%s.gsg-part-%s-", object, hex.EncodeToString(uid[:]))
	partName := func(i int) string {
		return fmt.Sprintf("%s%02d", prefix, i)
	}
	handles := make([]*storage.ObjectHandle, parts)
	// the service can only commit a part whose writer has been closed
	closing := make([]bool, parts)
	sums := make([]uint32, parts)
	lens := make([]int64, parts)
	var cause partFailure
	uploadCtx, abort := context.WithCancel(context.Background())
	defer abort()
	fail := func(i int, err error) {
		cause.record(i, err)
		abort()
	}
	var wg sync.WaitGroup
	for i := 0; i < parts; i++ {
		i := i
		wg.Add(1)
		ctx.Pool.AddWithDepth(1, func() {
			defer wg.Done()
			off := int64(i) * partSize
			length := partSize
			if off+length > size {
				length = size - off
			}
			pf := partFile(f)
			if pf != f {
				defer func() { _ = pf.Close() }()
			}
			wc := bkt.Object(partName(i)).NewWriter(uploadCtx)
			// the part's sum is taken in the copy that sends it, so the file is read once
			sum := crc32.New(common.Castagnoli)
			read, err := io.Copy(io.MultiWriter(wc, pb, sum), io.NewSectionReader(pf, off, length))
			if err == nil && read != length {
				// a section reader past a truncated end stops without an error;
				// the planned length must not fold into the object's checksum
				err = fmt.Errorf("part %d of %s is short: read %d of %d bytes, so the file was truncated while it was being uploaded", i, f.Name(), read, length)
			}
			if err != nil {
				fail(i, err)
				return
			}
			closing[i] = true
			if err := wc.Close(); err != nil {
				fail(i, err)
				return
			}
			handles[i] = bkt.Object(partName(i)).Generation(wc.Attrs().Generation)
			sums[i], lens[i] = sum.Sum32(), length
		})
	}
	wg.Wait()
	cleanup := func() []string {
		return deleteParts(handles, deletePart)
	}
	if i, err := cause.get(); err != nil {
		logger.Info(module, "upload object failed on part %d with %s, its parts are gs://%s/%s*", i, err, bucket, prefix)
		left := cleanup()
		var unknown []*storage.ObjectHandle
		for i := range handles {
			if closing[i] && handles[i] == nil {
				unknown = append(unknown, bkt.Object(partName(i)))
			}
		}
		var refused error
		late, unchecked := sweepLateParts(unknown, func(h *storage.ObjectHandle) (*storage.ObjectHandle, error) {
			pinned, err := lookupPart(h)
			if err != nil && refused == nil {
				refused = err
			}
			return pinned, err
		}, deletePart)
		if left = append(left, late...); len(left) > 0 {
			logger.Info(module, "upload parts of %s could not be deleted: %v", object, left)
		}
		if len(unchecked) > 0 {
			logger.Info(module, "could not check %d part(s) of %s with %s: %v", len(unchecked), object, refused, unchecked)
		}
		return err
	}
	crc, total := common.FoldCRC32C(sums, lens)
	// The fold is the checksum of exactly the bytes that were sent, so a file
	// rewritten under the parts no longer disagrees with itself and would be
	// composed, checksum and all, from two different files. The whole-file pass
	// that used to notice is what this removes; the cheap version is asked
	// instead, as oci does since #76, and before the compose, so nothing is
	// published. A rewrite keeping both size and mtime is invisible to it.
	after, err := f.Stat()
	if err == nil && (total != size || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime())) {
		err = fmt.Errorf("%s changed while its parts were being uploaded (%d bytes at %s, now %d bytes at %s): not composing an object from two different files",
			f.Name(), before.Size(), before.ModTime(), after.Size(), after.ModTime())
	}
	if err != nil {
		logger.Info(module, "upload object failed: %s", err)
		if left := cleanup(); len(left) > 0 {
			logger.Info(module, "upload parts of %s could not be deleted: %v", object, left)
		}
		return err
	}
	composer := bkt.Object(object).ComposerFrom(handles...)
	composer.Metadata = map[string]string{
		"goog-reserved-file-mtime": strconv.FormatInt(modTime.UnixNano(), 10),
	}
	// a single-stream upload leaves ContentType to the service, which sniffs the
	// first bytes; a composition gets no such detection
	composer.ContentType = sniffContentType(f)
	composer.CRC32C = crc
	composer.SendCRC32C = true
	_, err = composer.Run(context.Background())
	left := cleanup()
	if len(left) > 0 {
		// the object is already committed and verified: a retry of the upload would
		// only leave another set of parts, so this is reported, not returned
		logger.Info(module, "warning: upload parts of %s could not be deleted: %v", object, left)
	}
	if err != nil {
		logger.Info(module, "upload object failed when composing with %s", err)
		return err
	}
	return nil
}

// openSource is os.Open; a test replaces the path the moment it is opened
var openSource = os.Open

// partFile opens the file f has open once more, for one part to read through.
// The kernel keeps readahead state per descriptor: parts interleaved on one
// descriptor read as random to it and degrade to small synchronous reads.
// /proc/self/fd reopens the very file f has open even after its path was
// replaced, which is the file crc32cToSend describes; the path serves where
// there is no /proc and it still names that file. Failing both, f itself is
// shared: the same bytes, read slower.
func partFile(f *os.File) *os.File {
	want, err := f.Stat()
	if err != nil {
		return f
	}
	for _, name := range []string{fmt.Sprintf("/proc/self/fd/%d", f.Fd()), f.Name()} {
		pf, err := os.Open(name)
		if err != nil {
			continue
		}
		if got, err := pf.Stat(); err == nil && os.SameFile(want, got) {
			return pf
		}
		_ = pf.Close()
	}
	return f
}

// partFailure keeps the error of the part that stopped an attempt. The parts
// cancelled along fail after it, and not always with context.Canceled.
type partFailure struct {
	once sync.Once
	part int
	err  error
}

func (p *partFailure) record(part int, err error) {
	p.once.Do(func() {
		p.part, p.err = part, err
	})
}

// get is for after the parts have returned
func (p *partFailure) get() (int, error) {
	return p.part, p.err
}

// how long a part whose Close failed is looked for, and how often: the service
// can commit it after the attempt has returned, 0.5 s later in the case seen
var partSettleWindow = 5 * time.Second
var partSettlePoll = time.Second

// sweepLateParts removes the parts whose Close failed and that the service
// committed all the same. Each is looked up by name until it shows or the window
// closes, then deleted by the generation found: a delete by name would leave a
// noncurrent copy in a versioned bucket. A lookup that fails is a refusal, the
// library having retried the rest until the lookup's deadline, and is not
// repeated. It returns the parts that exist and could not be deleted, and the
// parts that could not be checked.
func sweepLateParts(unknown []*storage.ObjectHandle, lookup func(*storage.ObjectHandle) (*storage.ObjectHandle, error), del func(*storage.ObjectHandle) error) (left, unchecked []string) {
	pending := unknown
	for waited := time.Duration(0); len(pending) > 0; waited += partSettlePoll {
		var found, missing []*storage.ObjectHandle
		for _, h := range pending {
			pinned, err := lookup(h)
			switch {
			case err != nil:
				unchecked = append(unchecked, h.ObjectName())
			case pinned == nil:
				missing = append(missing, h)
			default:
				found = append(found, pinned)
			}
		}
		left = append(left, deleteParts(found, del)...)
		pending = missing
		if len(pending) == 0 || waited >= partSettleWindow {
			break
		}
		partDeleteSleep(partSettlePoll)
	}
	return left, unchecked
}

// lookupPart returns h pinned to the generation the service has, or nil when
// there is none.
func lookupPart(h *storage.ObjectHandle) (*storage.ObjectHandle, error) {
	ctx, cancel := context.WithTimeout(context.Background(), partLookupTimeout)
	defer cancel()
	attrs, err := h.Attrs(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return h.Generation(attrs.Generation), nil
}

// deletePart makes one attempt, with a deadline. Left to the library, a delete
// pinned to a generation is retried until the context ends and deleteParts'
// attempt limit is never reached.
func deletePart(h *storage.ObjectHandle) error {
	ctx, cancel := context.WithTimeout(context.Background(), partDeleteTimeout)
	defer cancel()
	return h.Retryer(storage.WithPolicy(storage.RetryNever)).Delete(ctx)
}

// deleteParts deletes the parts concurrently and returns the names of those still
// there. Only errors that may pass (408, 429, 5xx, no status at all) are tried again,
// with a wait between attempts; a refusal such as 403 or a hold is final at once.
var partDeleteBackoff = time.Second
var partDeleteSleep = time.Sleep

const partDeleteAttempts = 3

func transientDeleteError(err error) bool {
	var ge *googleapi.Error
	if errors.As(err, &ge) {
		return ge.Code == 408 || ge.Code == 429 || ge.Code >= 500
	}
	return true
}

func deleteParts(handles []*storage.ObjectHandle, del func(*storage.ObjectHandle) error) []string {
	failed := make([]bool, len(handles))
	var wg sync.WaitGroup
	for i, h := range handles {
		if h == nil {
			continue
		}
		wg.Add(1)
		go func(i int, h *storage.ObjectHandle) {
			defer wg.Done()
			for attempt := 1; ; attempt++ {
				err := del(h)
				if err == nil || errors.Is(err, storage.ErrObjectNotExist) {
					return
				}
				if attempt == partDeleteAttempts || !transientDeleteError(err) {
					failed[i] = true
					return
				}
				partDeleteSleep(time.Duration(attempt) * partDeleteBackoff)
			}
		}(i, h)
	}
	wg.Wait()
	var left []string
	for i, h := range handles {
		if failed[i] {
			left = append(left, h.ObjectName())
		}
	}
	return left
}

func sniffContentType(f *os.File) string {
	head := make([]byte, 512)
	n, _ := f.ReadAt(head, 0)
	return http.DetectContentType(head[:n])
}

// MoveObject moves an object
func (g *GCS) Move(srcBucket, srcPrefix, dstBucket, dstPrefix string) error {
	var err error
	if srcBucket == dstBucket && srcPrefix == dstPrefix {
		return nil
	}
	if err = g.Copy(srcBucket, srcPrefix, dstBucket, dstPrefix); err != nil {
		return err
	}
	if err = g.Delete(srcBucket, srcPrefix); err != nil {
		return err
	}
	return nil
}

// OutputObject outputs an object
func (g *GCS) Cat(bucket, prefix string) ([]byte, error) {
	var err error
	// create reader
	if err = g.Init(); err != nil {
		return nil, err
	}
	var rc io.ReadCloser
	if rc, err = g.client.Bucket(bucket).Object(prefix).NewReader(context.Background()); err != nil {
		logger.Info(module, "output object failed when create reader with %s", err)
		return nil, err
	}
	defer func() { _ = rc.Close() }()

	// write to bytes
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(rc)
	if err != nil {
		logger.Info(module, "output object failed when write to buffer with %s", err)
		return nil, err
	}
	return buf.Bytes(), nil
}

// IsObject checks if is an object
// case 1: gs://abc/def -> gs://abc/def/ : false
// case 2: gs://abc/de -> gs://abc/def/ : false
// case 3: gs://abc/def/ -> gs://abc/def/ : false
// case 4: gs://abc/def -> gs://abc/def : true
func (g *GCS) IsObject(bucket, prefix string) (bool, error) {
	var err error
	var obj *storage.ObjectAttrs
	if obj, err = g.GCSAttrs(bucket, prefix); err != nil {
		return false, err
	}
	return obj != nil, nil
}

// IsDirectory reports whether prefix has anything beneath it.
//
// It stops after the first entry or two. The question is existence, not
// contents, and answering it by walking the whole listing cost time
// proportional to the number of children -- measured against 1005 objects,
// 198ms before and 103ms after, and flat rather than growing. FileType calls
// this before nearly every command, so that landed on cp, rm, du, mv and
// rsync alike.
//
// The trailing slash is what makes the question mean "beneath". Without it the
// service matches on the raw prefix, so "edge/ab" would look like a directory
// because "edge/abc.txt" exists.
//
// The iterator yields a common prefix as an entry with an empty Name, so a
// directory whose children are all sub-directories still answers yes.
func (g *GCS) IsDirectory(bucket, prefix string) (bool, error) {
	if err := g.Init(); err != nil {
		return false, err
	}
	asDir := prefix
	if asDir != "" && !strings.HasSuffix(asDir, "/") {
		asDir += "/"
	}
	it := g.client.Bucket(bucket).Objects(context.Background(), &storage.Query{
		Prefix:    asDir,
		Delimiter: "/",
	})
	// Up to two entries, skipping one named exactly the prefix the caller
	// asked about. That name is not something beneath it, it is it -- the
	// zero-byte marker a console's "create folder" writes, which the listing
	// this replaced did not count either. Comparing against the caller's
	// prefix rather than asDir is what keeps "foo" and "foo/" answering
	// differently for a lone marker, exactly as they did before.
	//
	// The marker sorts before everything under it, so stopping at the first
	// entry would make a directory that also carries one look empty. Measured
	// against a real marker it arrives as Name; the Prefix arm is there
	// because the client documents that a trailing-slash object may be
	// reported either way.
	for i := 0; i < 2; i++ {
		attrs, err := it.Next()
		if err == iterator.Done {
			return false, nil
		}
		if err != nil {
			logger.Info(module, "cannot list gs://%s/%s: %s", bucket, asDir, err)
			return false, err
		}
		name := attrs.Name
		if name == "" {
			name = attrs.Prefix
		}
		if name != prefix {
			return true, nil
		}
	}
	return false, nil
}

// ParseFileModificationTimeMetadata parsed reserved modification time from metadata
func ParseFileModificationTimeMetadata(attrs *storage.ObjectAttrs) time.Time {
	if v, ok := attrs.Metadata["goog-reserved-file-mtime"]; ok {
		if len(v) > 0 {
			ts, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return time.Time{}
			}
			return time.Unix(0, ts)
		}
	}
	return time.Time{}
}

// GetFileModificationTime get file modification time
func GetFileModificationTime(attrs *storage.ObjectAttrs) time.Time {
	mt := ParseFileModificationTimeMetadata(attrs)
	if mt.Equal(time.Time{}) {
		mt = attrs.Updated
	}
	return mt
}

// MustEqualCRC32C compare CRC32C values if flag is set
// - compare a local file with an object from gcp
// - return an error if values are different
func (g *GCS) MustEqualCRC32C(flag bool, localPath, bucket, object string) error {
	if !flag {
		return nil
	}
	var err error
	var attr *storage.ObjectAttrs
	if attr, err = g.GCSAttrs(bucket, object); err != nil {
		return err
	}
	want := uint32(0)
	if attr != nil {
		want = attr.CRC32C
	}
	return mustMatchCRC32C(localPath, bucket, object, want)
}

// mustMatchCRC32C compares a local file against a checksum already in hand, so
// a download can settle against the generation its chunks read.
func mustMatchCRC32C(localPath, bucket, object string, want uint32) error {
	local := common.GetFileCRC32C(localPath)
	logger.Info(module, "CRC32C checking of local[%s] and bucket[%s] prefix[%s] are [%d] with [%d].",
		localPath, bucket, object, local, want)
	if local != want {
		log := fmt.Sprintf("CRC32C checking failed of local[%s] and bucket[%s] prefix[%s].", localPath, bucket, object)
		logger.Info(module, log)
		return fmt.Errorf(log)
	}
	logger.Info(module, "CRC32C checking success of local[%s] and bucket[%s] prefix[%s].", localPath, bucket, object)
	return nil
}

// crc32cToSend returns the checksum of exactly the bytes f will upload.
//
// It reads the handle rather than consulting the cached value for the path,
// and that is deliberate. The cache is keyed on path and mtime, so it is only
// as trustworthy as the assumption that content and mtime change together --
// and when that assumption is wrong the checksum sent describes different
// bytes than the body does. GCS recomputes on arrival and rejects the object,
// so the price of a stale entry is a whole upload spent to be told it was
// wrong. Measured against a 190MB file, hashing the handle costs 111ms cold
// and 50ms warm; a wasted upload of the same file costs orders of magnitude
// more, and fails work that should have succeeded.
//
// Reading the handle cannot be stale: it is the same descriptor the body is
// read from, so nothing can substitute the file underneath it.
//
// The handle is rewound afterwards, because the upload reads from where this
// left it.
func crc32cToSend(f *os.File, srcFile string) (uint32, error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	if _, err := io.Copy(h, f); err != nil {
		return 0, fmt.Errorf("cannot read %s to checksum it: %w", srcFile, err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("cannot rewind %s after checksumming it: %w", srcFile, err)
	}
	return h.Sum32(), nil
}
