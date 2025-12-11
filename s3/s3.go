package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
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
)

const (
	module = "S3"
)

type S3 struct {
	client *s3.Client
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
	var crc32c uint64 = 0
	if attrs.S3Attrs.Checksum != nil && attrs.S3Attrs.Checksum.ChecksumCRC32C != nil {
		crc32c, _ = strconv.ParseUint(*attrs.S3Attrs.Checksum.ChecksumCRC32C, 10, 32)
	}
	var size int64 = 0
	if attrs.S3Attrs.ObjectSize != nil {
		size = *attrs.S3Attrs.ObjectSize
	}
	return &system.Attrs{
		Size:    size,
		CRC32:   uint32(crc32c),
		ModTime: getR2ModificationTime(attrs),
	}
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
	if err = s.Init(bucket); err != nil {
		return nil, err
	}
	var oat types.ObjectAttributes
	if prefix == "" {
		return nil, nil
	}
	var attrs *s3.GetObjectAttributesOutput
	if attrs, err = s.client.GetObjectAttributes(context.TODO(), &s3.GetObjectAttributesInput{
		Bucket:           aws.String(bucket),
		Key:              aws.String(prefix),
		ObjectAttributes: oat.Values(),
	}); err != nil {
		logger.Debug(module, "failed with s3://%s/%s %s", bucket, prefix, err)
		return nil, nil
	}
	return &S3Attributes{
		S3Attrs: attrs,
		Bucket:  bucket,
		Prefix:  prefix,
	}, nil
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
	if err = s.Init(bucket); err != nil {
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
	index := 0
	for {
		if lo, err = s.client.ListObjectsV2(context.TODO(), &li); err != nil {
			logger.Info(module, "get objects attributes failed with %s", err)
			return nil, err
		}
		if !recursive {
			for _, cp := range lo.CommonPrefixes {
				commonPrefixes[*cp.Prefix] = struct{}{}
			}
		}
		if len(lo.Contents) == 0 {
			break
		}
		index++
		objects = append(objects, lo.Contents...)
		li.StartAfter = objects[len(objects)-1].Key
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

func (s *S3) batchAttrs(bucket, prefix string, recursive bool) ([]*S3Attributes, error) {
	var err error
	var subPaths []string
	if subPaths, err = s.listObjectsAndSubPaths(bucket, prefix, recursive); err != nil {
		return nil, err
	}
	res := make([]*S3Attributes, len(subPaths))
	errs := make([]error, len(subPaths))
	var wg sync.WaitGroup
	for index, subPath := range subPaths {
		if strings.HasSuffix(subPath, "/") {
			res[index] = &S3Attributes{
				S3Attrs: &s3.GetObjectAttributesOutput{},
				Bucket:  bucket,
				Prefix:  subPath,
			}
			continue
		}
		wg.Add(1)
		go func(index int, subPath string) {
			defer wg.Done()
			s3a, e := s.S3Attrs(bucket, subPath)
			res[index] = s3a
			errs[index] = e
		}(index, subPath)
	}
	wg.Wait()
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
		fos = append(fos, s.toFileObject(attr))
	}
	return fos, nil
}

// GetDiskUsageObjects gets disk usage of objects under a prefix
func (s *S3) DiskUsage(bucket, prefix string, recursive bool) ([]system.DiskUsage, error) {
	// is object
	var err error
	var obj *S3Attributes
	if obj, err = s.S3Attrs(bucket, prefix); err != nil {
		return nil, err
	}
	var size int64 = 0
	if obj.S3Attrs.ObjectSize != nil {
		size = *obj.S3Attrs.ObjectSize
	}
	if obj != nil {
		return []system.DiskUsage{{Size: size, Name: obj.Prefix}}, nil
	}
	// is directory
	root := system.NewDUTree(prefix, 0, true)
	var objs []*S3Attributes
	if objs, err = s.batchAttrs(bucket, prefix, recursive); err != nil {
		return nil, err
	}
	for _, obj := range objs {
		var size int64 = 0
		if obj.S3Attrs.ObjectSize != nil {
			size = *obj.S3Attrs.ObjectSize
		}
		du := system.NewDUTree(obj.Prefix, size, false)
		dirs := system.GetAllParents(du.Name, prefix)
		runningRoot := root
		for _, dir := range dirs[1:] {
			var pu *system.DUTree
			var exists bool
			if pu, exists = runningRoot.Children[dir]; !exists {
				pu = system.NewDUTree(dir, 0, true)
				runningRoot.Children[dir] = pu
			}
			runningRoot = pu
		}
		runningRoot.Children[du.Name] = du
	}
	return root.ToDiskUsages(), nil
}
func (s *S3) DeleteObject(bucket, prefix string) error {
	var err error
	if err = s.Init(bucket); err != nil {
		return err
	}
	_, err = s.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &prefix,
	})
	return err
}

// DeleteObject deletes an object
func (s *S3) Delete(bucket, prefix string) error {
	var err error
	if err = s.Init(bucket); err != nil {
		return err
	}
	if _, err = s.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
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
	if err = s.Init(srcBucket); err != nil {
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

	if _, err = s.client.CopyObject(context.TODO(), &s3.CopyObjectInput{
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
	if err = s.Init(bucket); err != nil {
		return err
	}
	if _, err = s.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(prefix),
		Body:   from,
	}); err != nil {
		return err
	}
	return nil
}

func (s *S3) Init(buckets ...string) error {
	if s.client != nil {
		return nil
	}
	if len(buckets) == 0 {
		common.Exit()
		return fmt.Errorf("S3 initialization need target bucket")
	}

	bucket := buckets[0]

	cfg, e1 := config.LoadDefaultConfig(context.TODO(), func(options *config.LoadOptions) error {
		var region string
		var err error
		if region, err = s3manager.GetBucketRegion(context.Background(), session.Must(session.NewSession()), bucket, "ap-southeast-1"); err != nil {
			return nil
		}
		options.Region = region
		return nil
	})
	if e1 != nil {
		logger.Info(module, "failed in loading defaultConfig with error: %s", e1)
		common.Exit()
		return e1
	}
	// Check if credentials are valid
	if _, err := cfg.Credentials.Retrieve(context.TODO()); err != nil {
		return err
	}

	s.client = s3.NewFromConfig(cfg)
	return nil
}

func (s *S3) GetObjectReader(bucket, prefix string) (io.ReadCloser, error) {
	var err error
	if err = s.Init(bucket); err != nil {
		return nil, err
	}
	var goo *s3.GetObjectOutput
	if goo, err = s.client.GetObject(context.TODO(), &s3.GetObjectInput{
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
	if err = s.Init(bucket); err != nil {
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
	if chunkSize <= 0 {
		chunkSize = int64(googleapi.DefaultUploadChunkSize)
	}
	chunkNumber := int(math.Ceil(float64(size) / float64(chunkSize)))
	if chunkNumber <= 0 {
		chunkNumber = 1
	}

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
				oo, oe := s.client.GetObject(context.TODO(), &gi)
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

				// write data with offset and length to file
				if _, we := io.Copy(io.MultiWriter(fl, pb), oo.Body); we != nil {
					logger.Info(module, "download object failed when write to offet with %s", we)
					common.Exit()
				}
				if err := fl.Sync(); err != nil {
					logger.Info(module, "download object failed when syncing to disk %s", err)
					common.Exit()
				}
			},
		)
	}

	// move back the temp file
	wg.Wait()
	err = os.Rename(dstFileTemp, dstFile)
	if err != nil {
		logger.Info(module, "download object failed when rename file with %s", err)
		return err
	}
	common.SetFileModificationTime(dstFile, getR2ModificationTime(attrs))
	return nil
}

// UploadObject uploads an object from a file
func (s *S3) Upload(srcFile, bucket, prefix string, ctx system.RunContext) error {
	var err error
	if err = s.Init(bucket); err != nil {
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
	if _, err = s.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(prefix),
		Body:   f,
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
	if err = s.Init(bucket); err != nil {
		return nil, err
	}
	var o *s3.GetObjectOutput
	if o, err = s.client.GetObject(context.TODO(), &s3.GetObjectInput{
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
func (s *S3) equalCRC32C(localPath, bucket, object string) (bool, error) {
	localCRC32C := common.GetFileCRC32C(localPath)
	r2CRC32C := uint32(0)
	var err error
	var attr *S3Attributes
	if attr, err = s.S3Attrs(bucket, object); err != nil {
		return false, err
	}
	if attr != nil {
		r2CRC32C = s.toAttrs(attr).CRC32
	}
	logger.Info(module, "CRC32C checking of local[%s] and bucket[%s] prefix[%s] are [%d] with [%d].",
		localPath, bucket, object, localCRC32C, r2CRC32C)
	return localCRC32C == r2CRC32C, nil
}

// MustEqualCRC32C compare CRC32C values if flag is set
// - compare a local file with an object from gcp
// - exit process if values are different
func (s *S3) MustEqualCRC32C(flag bool, localPath, bucket, object string) error {
	if !flag {
		return nil
	}
	var err error
	var ok bool
	if ok, err = s.equalCRC32C(localPath, bucket, object); err != nil {
		return err
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
func (s *S3) DoAttemptUnlock(bucket, object string, etag string) error {
	var err error
	if err = s.Init(bucket); err != nil {
		return err
	}
	// delete fails means other client has acquired lock or ETag changed
	logger.Debug(module, "DoAttemptUnlock: unlock with ETag:%s", etag)
	_, err = s.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
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
	if err = s.Init(bucket); err != nil {
		return "", err
	}

	// First, check if lock object already exists
	_, err = s.client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})

	if err == nil {
		// Lock object exists, check if it's expired
		attrs, err1 := s.S3Attrs(bucket, object)
		if err1 != nil {
			return "", err1
		}

		if attrs != nil && attrs.S3Attrs.LastModified != nil {
			// Check if lock is expired based on TTL
			if attrs.S3Attrs.LastModified.Add(ttl).Before(time.Now()) {
				logger.Debug(module, "DoAttemptLock expired. delete and try lock again")
				// Try to delete the expired lock
				_, _ = s.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(object),
				})
			} else {
				// Lock is still valid, return error
				return "", fmt.Errorf("lock already exists and not expired")
			}
		}
	}

	// Try to create the lock object
	putOutput, err := s.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Body:   strings.NewReader("1"),
	})

	if err != nil {
		return "", err
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
	if e1 := os.WriteFile(cacheFileName, []byte(etag), os.ModePerm); e1 != nil {
		logger.Info(module, "AttemptLock: cache lock ETag failed: %s", e1)
		return e1
	}
	return nil
}
