package s3

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nextbillion-ai/gsg/bar"
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"
	"google.golang.org/api/googleapi"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/smithy-go"
)

const (
	module = "S3"
	// legacyLockETag is the ETag every lock carried before each acquisition got
	// its own body: the content was always the single byte "1".
	legacyLockETag = `"c4ca4238a0b923820dcc509a6f75849b"`
	// lockCachePerm keeps the cache private. os.ModePerm made it world
	// readable and writable, and cross-user unlock cannot work anyway: the
	// ETag is specific to whoever acquired the lock.
	lockCachePerm = 0600
)

type S3 struct {
	// mu guards the lazily built clients. One S3 is registered for the whole
	// process and every worker goroutine reaches for a client, so this is
	// shared state.
	//
	// A client is per bucket rather than per process because its region is
	// fixed at construction. A single cached client meant the first bucket
	// touched decided the region for every later one, and a request for a
	// bucket elsewhere came back 301 MovedPermanently.
	mu      sync.Mutex
	clients map[string]*s3.Client
}

func (s *S3) Scheme() string {
	return "s3"
}

type S3Attributes struct {
	S3Attrs *s3.GetObjectAttributesOutput
	Bucket  string
	Prefix  string
}

func (s *S3) toAttrs(attrs *S3Attributes) *system.Attrs {
	if attrs == nil {
		return nil
	}
	if attrs.S3Attrs == nil {
		return nil
	}
	crc32c, _ := crc32cOf(attrs)
	var size int64 = 0
	if attrs.S3Attrs.ObjectSize != nil {
		size = *attrs.S3Attrs.ObjectSize
	}
	return &system.Attrs{
		Size:    size,
		CRC32:   crc32c,
		ModTime: getR2ModificationTime(attrs),
	}
}

// crc32cOf returns the object's CRC32C and whether it carries one.
//
// S3 reports a checksum as base64 of the raw bytes, big endian -- "SPPMDQ==",
// not a number. It was read with ParseUint base 10, which cannot parse that, so
// every object's checksum came back 0. A locally computed checksum therefore
// never matched, which is why an rsync from s3 re-downloaded every file on
// every run while the same rsync from gs reported no diff.
func crc32cOf(attrs *S3Attributes) (uint32, bool) {
	if attrs == nil || attrs.S3Attrs == nil || attrs.S3Attrs.Checksum == nil {
		return 0, false
	}
	// A multipart object's checksum is derived from its parts, not from the
	// whole object -- measured, single-part uploads report FULL_OBJECT and
	// multipart ones COMPOSITE. Comparing a composite against a whole-file
	// CRC32C would reject a perfectly good object and make rsync copy it again
	// every run, so only a whole-object checksum counts as comparable.
	if attrs.S3Attrs.Checksum.ChecksumType != types.ChecksumTypeFullObject {
		return 0, false
	}
	enc := attrs.S3Attrs.Checksum.ChecksumCRC32C
	if enc == nil || *enc == "" {
		return 0, false
	}
	raw, err := base64.StdEncoding.DecodeString(*enc)
	if err != nil || len(raw) != 4 {
		logger.Debug(module, "cannot read checksum %q", *enc)
		return 0, false
	}
	return binary.BigEndian.Uint32(raw), true
}

func getR2ModificationTime(attrs *S3Attributes) time.Time {
	if attrs.S3Attrs == nil {
		return time.Time{}
	}
	if attrs.S3Attrs.LastModified == nil {
		return time.Time{}
	}
	return *attrs.S3Attrs.LastModified
}

func (s *S3) toFileObject(attrs *S3Attributes) *system.FileObject {
	if attrs == nil {
		return nil
	}

	fo := &system.FileObject{
		System: s,
		Bucket: attrs.Bucket,
		Prefix: attrs.Prefix,
		Remote: true,
	}
	fo.SetAttributes(s.toAttrs(attrs))
	return fo
}

func (s *S3) S3Attrs(bucket, prefix string) (*S3Attributes, error) {
	var err error
	c, err := s.clientFor(bucket)
	if err != nil {
		return nil, err
	}
	var oat types.ObjectAttributes
	if prefix == "" {
		return nil, nil
	}
	var attrs *s3.GetObjectAttributesOutput
	if attrs, err = c.GetObjectAttributes(context.TODO(), &s3.GetObjectAttributesInput{
		Bucket:           aws.String(bucket),
		Key:              aws.String(prefix),
		ObjectAttributes: oat.Values(),
	}); err != nil {
		// Only a genuine absence is "not an object". Everything else --
		// throttling, auth, a region redirect, a network blip -- used to come
		// back as (nil, nil) too, so a request that failed was indistinguishable
		// from a key that is not there. Callers then treated the object as
		// absent, which is how a listing quietly loses entries and how a
		// cross-region 301 reads as "no such object".
		if isNotFound(err) {
			logger.Debug(module, "no object at s3://%s/%s", bucket, prefix)
			return nil, nil
		}
		logger.Info(module, "failed with s3://%s/%s %s", bucket, prefix, err)
		return nil, err
	}
	return &S3Attributes{
		S3Attrs: attrs,
		Bucket:  bucket,
		Prefix:  prefix,
	}, nil
}

// isNotFound reports whether err says the key does not exist, as opposed to
// saying the request could not be answered.
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	// Deliberately not "any 404". A missing bucket is a 404 too -- measured,
	// code NoSuchBucket -- and that is a failure to answer, not an absent key.
	// Matching on the code keeps the two apart where the modelled types do not
	// reach us.
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	return false
}

// GetObjectAttributes gets the attributes of an object
func (s *S3) Attributes(bucket, prefix string) (*system.Attrs, error) {
	var err error
	var s3a *S3Attributes
	if s3a, err = s.S3Attrs(bucket, prefix); err != nil {
		return nil, err
	}
	return s.toAttrs(s3a), nil
}

/*
var (
	subFileTest   = regexp.MustCompile(`^/?[^/]+$`)
	subFolderTest = regexp.MustCompile(`^/?([^/]+/).*`)
)

func matchImmediateSubPath(prefix, path string) string {
	testPath := strings.Replace(path, prefix, "", 1)
	if match := subFileTest.FindStringSubmatch(testPath); len(match) > 0 {
		//fmt.Printf("subFile: %s, %s, %s\n", prefix, path, testPath)
		return path
	}
	if match := subFolderTest.FindStringSubmatch(testPath); len(match) > 0 {
		//fmt.Printf("subFolder: %s, %s, %s, %s\n", prefix, path, testPath, match[1])
		return prefix + match[1]
	}
	return ""
}
*/

func (s *S3) listObjectsAndSubPaths(bucket, prefix string, recursive bool) ([]string, error) {
	var err error
	c, err := s.clientFor(bucket)
	if err != nil {
		return nil, err
	}
	var ok bool
	if ok, err = s.IsObject(bucket, prefix); err != nil {
		return nil, err
	}
	if !ok {
		prefix = common.SetPrefixAsDirectory(prefix)
	}
	li := s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String(""),
	}
	if !recursive {
		li.Delimiter = aws.String("/")
	}
	var lo *s3.ListObjectsV2Output
	objects := []types.Object{}
	commonPrefixes := map[string]struct{}{}
	for {
		if lo, err = c.ListObjectsV2(context.TODO(), &li); err != nil {
			logger.Info(module, "get objects attributes failed with %s", err)
			return nil, err
		}
		if !recursive {
			for _, cp := range lo.CommonPrefixes {
				commonPrefixes[*cp.Prefix] = struct{}{}
			}
		}
		objects = append(objects, lo.Contents...)

		// IsTruncated is the only thing that says whether more pages exist. The
		// loop used to stop on an empty Contents instead, which is wrong for a
		// delimited listing: a page whose keys all collapsed into common
		// prefixes carries no Contents at all, so a prefix with more than one
		// page of subdirectories stopped after the first and the rest were
		// silently missing.
		if lo.IsTruncated == nil || !*lo.IsTruncated {
			break
		}
		// And continue with the token the server gave us. StartAfter is a
		// starting key, not a cursor: it ignores common prefixes entirely, so
		// it cannot resume a delimited listing.
		//
		// A truncated page must carry a token, and a new one, or there is no
		// way forward. AWS always does; failing here rather than trusting it
		// means a provider that does not turns into an error instead of a loop
		// that re-fetches one page until it runs out of memory.
		if lo.NextContinuationToken == nil || *lo.NextContinuationToken == "" {
			return nil, fmt.Errorf("listing bucket[%s] prefix[%s] was truncated without a continuation token", bucket, prefix)
		}
		if li.ContinuationToken != nil && *li.ContinuationToken == *lo.NextContinuationToken {
			return nil, fmt.Errorf("listing bucket[%s] prefix[%s] repeated its continuation token", bucket, prefix)
		}
		li.ContinuationToken = lo.NextContinuationToken
	}

	subPaths := []string{}

	for _, o := range objects {
		subPaths = append(subPaths, *o.Key)
	}
	if !recursive {
		for cp := range commonPrefixes {
			subPaths = append(subPaths, cp)
		}
	}
	return subPaths, nil
}

// maxAttrsInFlight caps concurrent GetObjectAttributes calls while listing.
// It matches the default of the -c flag; batchAttrs has no access to the
// worker pool, so it cannot follow that flag directly.
const maxAttrsInFlight = 64

func (s *S3) batchAttrs(bucket, prefix string, recursive bool) ([]*S3Attributes, error) {
	var err error
	var subPaths []string
	if subPaths, err = s.listObjectsAndSubPaths(bucket, prefix, recursive); err != nil {
		return nil, err
	}
	res := make([]*S3Attributes, len(subPaths))
	errs := make([]error, len(subPaths))

	// A sub-path ending in "/" is a common prefix rather than an object, so it
	// needs no request and is filled in right here, exactly as before. Only the
	// entries that actually cost a round trip go through the fan-out below.
	// Sized for the common case, where most sub-paths are objects rather than
	// common prefixes: this is the million-key path, and growing by doubling
	// would copy it repeatedly.
	fetch := make([]int, 0, len(subPaths))
	for index, subPath := range subPaths {
		if strings.HasSuffix(subPath, "/") {
			res[index] = &S3Attributes{
				S3Attrs: &s3.GetObjectAttributesOutput{},
				Bucket:  bucket,
				Prefix:  subPath,
			}
			continue
		}
		fetch = append(fetch, index)
	}

	// One goroutine per object also meant one in-flight GetObjectAttributes
	// call per object, so a prefix holding a million keys started a million of
	// each at once. Bound both.
	common.ParallelDo(len(fetch), maxAttrsInFlight, func(i int) {
		index := fetch[i]
		s3a, e := s.S3Attrs(bucket, subPaths[index])
		res[index] = s3a
		errs[index] = e
	})
	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}
	return res, nil

}

// GetObjectsAttributes gets the attributes of all the objects under a prefix
func (s *S3) BatchAttributes(bucket, prefix string, recursive bool) ([]*system.Attrs, error) {
	res := []*system.Attrs{}
	var err error
	var s3as []*S3Attributes
	if s3as, err = s.batchAttrs(bucket, prefix, recursive); err != nil {
		return nil, err
	}
	for _, attr := range s3as {
		res = append(res, s.toAttrs(attr))
	}
	return res, nil
}

// List objects under a prefix
func (s *S3) List(bucket, prefix string, recursive bool) ([]*system.FileObject, error) {
	fos := []*system.FileObject{}
	var err error
	var s3as []*S3Attributes
	if s3as, err = s.batchAttrs(bucket, prefix, recursive); err != nil {
		return nil, err
	}
	for _, attr := range s3as {
		fo := s.toFileObject(attr)
		if fo == nil {
			// batchAttrs leaves a nil entry when an object's attribute lookup
			// failed. Callers dereference what List returns, so drop it here --
			// but say so, because S3Attrs reports every failure as "not an
			// object" and a dropped key would otherwise go unnoticed.
			logger.Info(module, "skipping bucket[%s] prefix[%s]: attributes unavailable", bucket, prefix)
			continue
		}
		fos = append(fos, fo)
	}
	return fos, nil
}

// s3ObjectSize reads an object's size, which is absent on directory markers and
// on responses that carry no ObjectSize.
func s3ObjectSize(attrs *S3Attributes) int64 {
	if attrs == nil || attrs.S3Attrs == nil || attrs.S3Attrs.ObjectSize == nil {
		return 0
	}
	return *attrs.S3Attrs.ObjectSize
}

// GetDiskUsageObjects gets disk usage of objects under a prefix
func (s *S3) DiskUsage(bucket, prefix string, recursive bool) ([]system.DiskUsage, error) {
	// is object
	var err error
	var obj *S3Attributes
	if obj, err = s.S3Attrs(bucket, prefix); err != nil {
		return nil, err
	}
	// S3Attrs reports "not an object" as (nil, nil), so this must be checked
	// before anything is read off obj.
	if obj != nil {
		return []system.DiskUsage{{Size: s3ObjectSize(obj), Name: obj.Prefix}}, nil
	}
	// is directory
	root := system.NewDUTree(prefix, 0, true)
	var objs []*S3Attributes
	if objs, err = s.batchAttrs(bucket, prefix, recursive); err != nil {
		return nil, err
	}
	for _, obj := range objs {
		if obj == nil {
			// Same as in List: S3Attrs reports every failure as "not an
			// object", so an unreadable key would silently undercount the total.
			logger.Info(module, "skipping an object under bucket[%s] prefix[%s]: attributes unavailable", bucket, prefix)
			continue
		}
		root.Add(obj.Prefix, s3ObjectSize(obj), prefix)
	}
	return root.ToDiskUsages(), nil
}
func (s *S3) DeleteObject(bucket, prefix string) error {
	var err error
	c, err := s.clientFor(bucket)
	if err != nil {
		return err
	}
	_, err = c.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &prefix,
	})
	return err
}

// DeleteObject deletes an object
func (s *S3) Delete(bucket, prefix string) error {
	var err error
	c, err := s.clientFor(bucket)
	if err != nil {
		return err
	}
	if _, err = c.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &prefix,
	}); err != nil {
		logger.Info(module, "delete object r2://%s/%s failed with %s", bucket, prefix, err)
		return err
	}
	logger.Info(module, "Removing bucket[%s] prefix[%s]", bucket, prefix)
	return nil
}

// CopyObject copies an object
func (s *S3) Copy(srcBucket, srcPrefix, dstBucket, dstPrefix string) error {
	var err error
	// The destination's client: CopyObject is issued against the bucket
	// being written to, and using the source's client sent it to the wrong
	// endpoint whenever the two lived in different regions.
	c, err := s.clientFor(dstBucket)
	if err != nil {
		return err
	}
	var s3a *S3Attributes
	if s3a, err = s.S3Attrs(srcBucket, srcPrefix); err != nil {
		return err
	}
	// check object
	if s3a == nil {
		log := fmt.Sprintf("failed with bucket[%s] prefix[%s] not an object", srcBucket, srcPrefix)
		logger.Debug(module, log)
		return fmt.Errorf(log)
	}

	if _, err = c.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(dstBucket),
		Key:        aws.String(dstPrefix),
		CopySource: aws.String(fmt.Sprintf("%v/%v", srcBucket, srcPrefix)),
	}); err != nil {
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

func (s *S3) PutObject(bucket, prefix string, from io.Reader) error {
	var err error
	c, err := s.clientFor(bucket)
	if err != nil {
		return err
	}
	if _, err = c.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(prefix),
		Body:              from,
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
	}); err != nil {
		return err
	}
	return nil
}

// Init satisfies ISystem. It exists to fail early on a bad bucket or missing
// credentials; the client it builds is kept for clientFor to hand out.
func (s *S3) Init(buckets ...string) error {
	if len(buckets) == 0 {
		common.Exit()
		return fmt.Errorf("S3 initialization need target bucket")
	}
	_, err := s.clientFor(buckets[0])
	return err
}

// clientFor returns the client for one bucket, building it on first use.
func (s *S3) clientFor(bucket string) (*s3.Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.clients[bucket]; ok {
		return c, nil
	}

	// The bucket's own region, and a failure to find it is a failure to build
	// the client. This used to return nil from the callback, leaving the
	// fallback region in place, so a lookup that failed produced a client
	// quietly pointed at the wrong place.
	region, rerr := s3manager.GetBucketRegion(context.Background(), session.Must(session.NewSession()), bucket, "ap-southeast-1")
	if rerr != nil {
		logger.Info(module, "cannot determine the region of bucket[%s]: %s", bucket, rerr)
		return nil, rerr
	}
	cfg, e1 := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if e1 != nil {
		logger.Info(module, "failed in loading defaultConfig with error: %s", e1)
		common.Exit()
		return nil, e1
	}
	// Check if credentials are valid
	if _, err := cfg.Credentials.Retrieve(context.TODO()); err != nil {
		return nil, err
	}

	c := s3.NewFromConfig(cfg)
	if s.clients == nil {
		s.clients = map[string]*s3.Client{}
	}
	s.clients[bucket] = c
	logger.Debug(module, "built a client for bucket[%s] in region[%s]", bucket, region)
	return c, nil
}

func (s *S3) GetObjectReader(bucket, prefix string) (io.ReadCloser, error) {
	var err error
	c, err := s.clientFor(bucket)
	if err != nil {
		return nil, err
	}
	var goo *s3.GetObjectOutput
	if goo, err = c.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(prefix)}); err != nil {
		return nil, err
	}
	return goo.Body, nil
}

// DownloadObjectWithWorkerPool downloads a specific byte range of an object to a file.
func (s *S3) Download(
	bucket, prefix, dstFile string,
	forceChecksum bool,
	ctx system.RunContext,
) error {
	var err error
	c, err := s.clientFor(bucket)
	if err != nil {
		return err
	}
	var attrs *S3Attributes
	// check object
	if attrs, err = s.S3Attrs(bucket, prefix); err != nil {
		return err
	}
	if attrs == nil {
		log := fmt.Sprintf("failed with bucket[%s] prefix[%s] not an object", bucket, prefix)
		logger.Debug(module, log)
		return fmt.Errorf(log)
	}
	var size int64 = 0
	if attrs.S3Attrs.ObjectSize != nil {
		size = *attrs.S3Attrs.ObjectSize
	}
	chunkSize := ctx.ChunkSize
	if chunkSize < 0 {
		chunkSize = int64(googleapi.DefaultUploadChunkSize)
	} else if chunkSize == 0 {
		// chunk size 0 means no chunking, download as single chunk
		chunkSize = size
		if chunkSize <= 0 {
			chunkSize = 1
		}
	}
	chunkNumber := int(math.Ceil(float64(size) / float64(chunkSize)))
	if chunkNumber <= 0 {
		chunkNumber = 1
	}
	logger.Debug(module, "Downloading [%s] with %d chunk(s), chunk size: %d bytes, total size: %d bytes", prefix, chunkNumber, chunkSize, size)

	var pb *bar.ProgressBar
	var wg sync.WaitGroup
	var once sync.Once
	dstFileTemp := common.GetTempFile(dstFile)
	for i := 0; i < chunkNumber; i++ {

		// decide offset and length
		startByte := int64(i) * chunkSize
		length := chunkSize
		if i == chunkNumber-1 {
			length = size - startByte
		}

		wg.Add(1)
		ctx.Pool.AddWithDepth(1,
			func() {
				defer wg.Done()

				// create folder and temp file if not exist
				once.Do(func() {
					pb = ctx.Bars.New(size, fmt.Sprintf("Downloading [%s]:", prefix))
					folder, _ := common.ParseFile(dstFile)
					if !common.IsPathExist(folder) {
						common.CreateFolder(folder)
					}
					common.CreateFile(dstFileTemp, size)
				})
				gi := s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(prefix),
					Range:  aws.String(fmt.Sprintf("bytes=%d-%d", startByte, startByte+length)),
				}
				if forceChecksum {
					gi.ChecksumMode = types.ChecksumModeEnabled
				}
				oo, oe := c.GetObject(context.TODO(), &gi)
				if oe != nil {
					logger.Info(module, "download object failed when create reader with %s", oe)
					common.Exit()
				}

				// create write with offset and length of file
				fl, _ := os.OpenFile(dstFileTemp, os.O_WRONLY, 0766)
				_, se := fl.Seek(startByte, 0)
				if se != nil {
					logger.Info(module, "download object failed when seek for offset with %s", se)
					common.Exit()
				}

				defer func() { _ = fl.Close() }()

				// If gentle I/O mode, use throttled writer to reduce impact
				if ctx.GentleIO {
					logger.Debug(module, "Using gentle I/O mode with throttled writer for chunk at offset %d", startByte)
					common.FadviseWriteSequential(fl)

					// Use throttled copy: write in small chunks with delays
					buf := make([]byte, 1*1024*1024) // 1MB buffer
					totalWritten := int64(0)

					for {
						n, readErr := oo.Body.Read(buf)
						if n > 0 {
							if _, writeErr := fl.Write(buf[:n]); writeErr != nil {
								logger.Info(module, "download object failed when write: %s", writeErr)
								common.Exit()
							}
							if _, writeErr := pb.Write(buf[:n]); writeErr != nil {
								// Progress bar write error, non-fatal
							}
							totalWritten += int64(n)

							// Every 10MB, pause and drop cache
							if totalWritten%(10*1024*1024) == 0 {
								common.FadviseWriteDontNeed(fl, startByte, totalWritten)
								time.Sleep(time.Millisecond * 20) // 20ms pause every 10MB
							}
						}
						if readErr == io.EOF {
							break
						}
						if readErr != nil {
							logger.Info(module, "download object failed when read: %s", readErr)
							common.Exit()
						}
					}

					// Final fadvise to drop remaining data
					common.FadviseWriteDontNeed(fl, startByte, totalWritten)
				} else {
					// Fast mode: use buffered writer
					bufWriter := bufio.NewWriterSize(fl, 4*1024*1024)
					defer func() { _ = bufWriter.Flush() }()

					if _, we := io.Copy(io.MultiWriter(bufWriter, pb), oo.Body); we != nil {
						logger.Info(module, "download object failed when write to offet with %s", we)
						common.Exit()
					}

					if err := bufWriter.Flush(); err != nil {
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
	common.SetFileModificationTime(dstFile, getR2ModificationTime(attrs))
	// The point of -v. forceChecksum only ever set ChecksumMode on the request
	// and then discarded the answer; MustEqualCRC32C was defined and called
	// from nowhere, so the flag verified nothing on this backend.
	if err = s.MustEqualCRC32C(forceChecksum, dstFile, bucket, prefix); err != nil {
		return err
	}
	return nil
}

// UploadObject uploads an object from a file
func (s *S3) Upload(srcFile, bucket, prefix string, ctx system.RunContext) error {
	var err error
	c, err := s.clientFor(bucket)
	if err != nil {
		return err
	}
	// open source file
	f, err := os.Open(srcFile)
	if err != nil {
		logger.Info(module, "upload object failed when open file with %s", err)
		return err
	}
	defer func() { _ = f.Close() }()

	// progress bar
	//modTime := common.GetFileModificationTime(srcFile)
	logger.Info(module, "uploading %s to %s/%s", srcFile, bucket, prefix)
	// upload file
	// Ask for CRC32C specifically. The SDK otherwise picks its own algorithm --
	// CRC32 today -- and everything here compares against a locally computed
	// CRC32C, so an object uploaded without this carries a checksum nothing in
	// gsg can use.
	if _, err = c.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(prefix),
		Body:              f,
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
	}); err != nil {
		logger.Info(module, "upload object failed when copy file with %s", err)
		return err
	}
	return nil
}

// MoveObject moves an object
func (s *S3) Move(srcBucket, srcPrefix, dstBucket, dstPrefix string) error {
	if srcBucket == dstBucket && srcPrefix == dstPrefix {
		return nil
	}
	var err error
	if err = s.Copy(srcBucket, srcPrefix, dstBucket, dstPrefix); err != nil {
		return err
	}
	if err = s.Delete(srcBucket, srcPrefix); err != nil {
		return err
	}
	return nil
}

// OutputObject outputs an object
func (s *S3) Cat(bucket, prefix string) ([]byte, error) {
	var err error
	// create reader
	c, err := s.clientFor(bucket)
	if err != nil {
		return nil, err
	}
	var o *s3.GetObjectOutput
	if o, err = c.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(prefix),
	}); err != nil {
		logger.Info(module, "output object failed when create reader with %s", err)
		return nil, err
	}

	// write to bytes
	buf := new(bytes.Buffer)
	if _, err = buf.ReadFrom(o.Body); err != nil {
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
func (s *S3) IsObject(bucket, prefix string) (bool, error) {
	var err error
	var s3a *S3Attributes
	if s3a, err = s.S3Attrs(bucket, prefix); err != nil {
		return false, err
	}
	return s3a != nil, nil
}

// IsDirectory checks if is a directory
func (s *S3) IsDirectory(bucket, prefix string) (bool, error) {
	var err error
	var objs []string
	if objs, err = s.listObjectsAndSubPaths(bucket, prefix, true); err != nil {
		return false, err
	}
	if len(objs) > 1 {
		return true, nil
	}
	if len(objs) == 1 {
		return len(objs[0]) > len(prefix), nil
	}
	return false, nil
}

// equalCRC32C return true if CRC32C values are the same
// - compare a local file with an object from gcp
// equalCRC32C compares a local file against the object's stored checksum. The
// second result says whether there was a checksum to compare with at all:
// objects written before gsg asked for CRC32C carry a different algorithm or
// none, and reading that as zero would reject every one of them.
func (s *S3) equalCRC32C(localPath, bucket, object string) (equal, comparable bool, err error) {
	var attr *S3Attributes
	if attr, err = s.S3Attrs(bucket, object); err != nil {
		return false, false, err
	}
	if attr == nil {
		// Verification was asked for and the object is not there. That is a
		// failure, not something to pass over.
		return false, false, fmt.Errorf("cannot verify s3://%s/%s: no such object", bucket, object)
	}
	remote, ok := crc32cOf(attr)
	if !ok {
		return false, false, nil
	}
	local := common.GetFileCRC32C(localPath)
	logger.Info(module, "CRC32C checking of local[%s] and bucket[%s] prefix[%s] are [%d] with [%d].",
		localPath, bucket, object, local, remote)
	return local == remote, true, nil
}

// MustEqualCRC32C compare CRC32C values if flag is set
// - compare a local file with an object from gcp
// - exit process if values are different
func (s *S3) MustEqualCRC32C(flag bool, localPath, bucket, object string) error {
	if !flag {
		return nil
	}
	var err error
	var ok, comparable bool
	if ok, comparable, err = s.equalCRC32C(localPath, bucket, object); err != nil {
		return err
	}
	if !comparable {
		// Nothing to check against. Saying so is the honest outcome: failing
		// would reject every object written before gsg asked for CRC32C, and
		// passing silently is what this flag already did.
		logger.Info(module, "CRC32C checking skipped for bucket[%s] prefix[%s]: no CRC32C stored", bucket, object)
		return nil
	}
	if !ok {
		log := fmt.Sprintf("CRC32C checking failed of local[%s] and bucket[%s] prefix[%s].", localPath, bucket, object)
		logger.Info(module, log)
		return fmt.Errorf(log)
	}
	logger.Info(module, "CRC32C checking success of local[%s] and bucket[%s] prefix[%s].", localPath, bucket, object)
	return nil
}

// DoAttemptUnlock takes ETag as input and returns potential error
// newLockToken returns a value unique to one lock acquisition, so that the
// object's ETag identifies this lock rather than merely the fact that a lock
// exists.
func newLockToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("cannot generate a lock token: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// validLockETag reports whether etag is shaped like the quoted entity-tag S3
// returns from PutObject, which is what AttemptLock stores verbatim. Anything
// else -- empty, "*", a bare hash, something carrying CR or LF -- is refused
// rather than normalised, because the only thing this value is used for is
// proving the lock belongs to the caller.
func validLockETag(etag string) bool {
	if len(etag) < 3 || etag[0] != '"' || etag[len(etag)-1] != '"' {
		return false
	}
	return !strings.ContainsAny(etag, "*\r\n")
}

func (s *S3) DoAttemptUnlock(bucket, object string, etag string) error {
	var err error
	c, err := s.clientFor(bucket)
	if err != nil {
		return err
	}
	// A receipt that is not a well formed entity-tag proves nothing, so there is
	// no safe delete to make. Empty is the common case -- a receipt missing, or
	// truncated by a run that died mid-write, which the other backends already
	// treat as an error. "*" is the dangerous one: If-Match: * matches any
	// object, so a receipt corrupted to that would delete whoever holds the
	// lock, which is the very thing this condition exists to prevent.
	// The ETag every lock shared before acquisitions carried a distinct body.
	// A receipt holding it cannot identify one lock, so it is refused: during a
	// mixed rollout it would otherwise delete any lock an older gsg still holds.
	if etag == legacyLockETag {
		logger.Info(module, "DoAttemptUnlock: receipt for s3://%s/%s predates unique lock bodies", bucket, object)
		return fmt.Errorf("cannot release the lock on s3://%s/%s: its receipt predates unique lock bodies and cannot identify one lock", bucket, object)
	}
	if !validLockETag(etag) {
		logger.Info(module, "DoAttemptUnlock: unusable ETag %q for s3://%s/%s", etag, bucket, object)
		return fmt.Errorf("cannot release the lock on s3://%s/%s: %q is not an ETag that proves it is ours", bucket, object, etag)
	}
	// delete fails means other client has acquired lock or ETag changed
	logger.Debug(module, "DoAttemptUnlock: unlock with ETag:%s", etag)
	// If-Match is what makes that comment true. Without it the delete removed
	// whichever lock happened to be present -- including one another process
	// had just acquired, seconds after this caller's own lock expired. The gcs
	// backend has always conditioned its delete on the generation it stored;
	// this is the same guarantee.
	_, err = c.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(object),
		IfMatch: aws.String(etag),
	})
	return err
}

// AttemptUnLock attempts to release a remote lock file
func (s *S3) AttemptUnLock(bucket, object string) error {
	cacheFileName := common.GenTempFileName(bucket, "/", object)
	etagBytes, e := os.ReadFile(cacheFileName)
	if e != nil {
		logger.Debug(module, "failed to read lock cache: %+v", cacheFileName)
		return nil
	}
	etag := string(etagBytes)
	if e := s.DoAttemptUnlock(bucket, object, etag); e != nil {
		logger.Debug(module, "unlock error: %+v", e)
		return e
	}
	return nil
}

// DoAttemptLock returns ETag and potential error
func (s *S3) DoAttemptLock(bucket, object string, ttl time.Duration) (string, error) {
	var err error
	c, err := s.clientFor(bucket)
	if err != nil {
		return "", err
	}

	// An existing lock is either still held, in which case we lose, or expired,
	// in which case it is cleared out of the way -- conditionally, so that a
	// lock someone else acquired between the read and the delete survives.
	head, herr := c.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	if herr == nil {
		if head.LastModified != nil && !head.LastModified.Add(ttl).Before(time.Now()) {
			return "", fmt.Errorf("lock already exists and not expired")
		}
		logger.Debug(module, "DoAttemptLock: clearing an expired lock")
		del := &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(object)}
		if head.ETag != nil {
			del.IfMatch = head.ETag
		}
		if _, derr := c.DeleteObject(context.TODO(), del); derr != nil {
			// Someone else cleared or replaced it first. Theirs now.
			logger.Debug(module, "DoAttemptLock: expired lock changed underneath us: %s", derr)
			return "", fmt.Errorf("lock already exists and not expired")
		}
	}

	// A distinct body per acquisition, so the ETag identifies THIS lock. The
	// ETag is derived from the content, so while every lock held the same byte
	// every lock had the same ETag, and the If-Match in DoAttemptUnlock matched
	// anybody's lock rather than this one -- the very thing it is there to
	// prevent. GCS needs no equivalent: a generation is unique per write
	// regardless of what was written.
	token, terr := newLockToken()
	if terr != nil {
		return "", terr
	}
	// If-None-Match: * creates only when the key is absent, so exactly one of
	// several contenders wins. Without it both the Head above and this Put were
	// unconditional, and every contender came away believing it held the lock.
	putOutput, err := c.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(object),
		Body:        strings.NewReader(token),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		// A 412 here means another contender created it first, which is a lost
		// race rather than a failure of this call.
		logger.Debug(module, "DoAttemptLock: could not create the lock: %s", err)
		return "", fmt.Errorf("lock already exists and not expired")
	}

	// Successfully acquired lock, return ETag
	if putOutput.ETag != nil {
		return *putOutput.ETag, nil
	}
	return "", fmt.Errorf("failed to get ETag from put operation")
}

// AttemptLock attempts to write a remote lock file
func (s *S3) AttemptLock(bucket, object string, ttl time.Duration) error {
	etag, e := s.DoAttemptLock(bucket, object, ttl)
	if e != nil {
		logger.Info(module, "attempt lock failed: %s", e)
		return e
	}

	// Upon successful write, store ETag in /tmp
	logger.Debug(module, "AttemptLock: storing ETag: %+v", etag)
	cacheFileName := common.GenTempFileName(bucket, "/", object)
	if e1 := common.WriteFileAtomic(cacheFileName, []byte(etag), lockCachePerm); e1 != nil {
		logger.Info(module, "AttemptLock: cache lock ETag failed: %s", e1)
		return e1
	}
	return nil
}
