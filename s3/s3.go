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
	return &system.Attrs{
		Size:    attrs.S3Attrs.ObjectSize,
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

// storageClient gets or creates a gcp storage client
func (s *S3) init(bucket string) {
	if s.client != nil {
		return
	}

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
	}

	s.client = s3.NewFromConfig(cfg)
}

func (s *S3) S3Attrs(bucket, prefix string) *S3Attributes {
	s.init(bucket)
	var oat types.ObjectAttributes
	if prefix == "" {
		return nil
	}
	attrs, err := s.client.GetObjectAttributes(context.TODO(), &s3.GetObjectAttributesInput{
		Bucket:           aws.String(bucket),
		Key:              aws.String(prefix),
		ObjectAttributes: oat.Values(),
	})
	if err != nil {
		logger.Debug(module, "failed with s3://%s/%s %s", bucket, prefix, err)
		return nil
	}
	return &S3Attributes{
		S3Attrs: attrs,
		Bucket:  bucket,
		Prefix:  prefix,
	}
}

// GetObjectAttributes gets the attributes of an object
func (s *S3) Attributes(bucket, prefix string) *system.Attrs {
	return s.toAttrs(s.S3Attrs(bucket, prefix))
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

func (s *S3) listObjectsAndSubPaths(bucket, prefix string, recursive bool) []string {
	s.init(bucket)
	if !s.IsObject(bucket, prefix) {
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
	var le error
	objects := []types.Object{}
	commonPrefixes := map[string]struct{}{}
	index := 0
	for {
		if lo, le = s.client.ListObjectsV2(context.TODO(), &li); le != nil {
			logger.Info(module, "get objects attributes failed with %s", le)
			common.Exit()
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
	return subPaths
}

func (s *S3) batchAttrs(bucket, prefix string, recursive bool) []*S3Attributes {
	subPaths := s.listObjectsAndSubPaths(bucket, prefix, recursive)
	res := make([]*S3Attributes, len(subPaths))
	var wg sync.WaitGroup
	for index, subPath := range subPaths {
		if strings.HasSuffix(subPath, "/") {
			res[index] = &S3Attributes{
				Bucket: bucket,
				Prefix: subPath,
			}
			continue
		}
		wg.Add(1)
		go func(index int, subPath string) {
			defer wg.Done()
			res[index] = s.S3Attrs(bucket, subPath)
		}(index, subPath)
	}
	wg.Wait()
	return res

}

// GetObjectsAttributes gets the attributes of all the objects under a prefix
func (s *S3) BatchAttributes(bucket, prefix string, recursive bool) []*system.Attrs {
	res := []*system.Attrs{}
	for _, attr := range s.batchAttrs(bucket, prefix, recursive) {
		res = append(res, s.toAttrs(attr))
	}
	return res
}

// List objects under a prefix
func (s *S3) List(bucket, prefix string, recursive bool) []*system.FileObject {
	fos := []*system.FileObject{}
	for _, attr := range s.batchAttrs(bucket, prefix, recursive) {
		fos = append(fos, s.toFileObject(attr))
	}
	return fos
}

// GetDiskUsageObjects gets disk usage of objects under a prefix
func (s *S3) DiskUsage(bucket, prefix string, recursive bool) []system.DiskUsage {
	// is object
	obj := s.S3Attrs(bucket, prefix)
	if obj != nil {
		return []system.DiskUsage{{Size: obj.S3Attrs.ObjectSize, Name: obj.Prefix}}
	}
	// is directory
	root := system.NewDUTree(prefix, 0, true)
	objs := s.batchAttrs(bucket, prefix, recursive)
	for _, obj := range objs {
		du := system.NewDUTree(obj.Prefix, obj.S3Attrs.ObjectSize, false)
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
	return root.ToDiskUsages()
}

// DeleteObject deletes an object
func (s *S3) Delete(bucket, prefix string) {
	s.init(bucket)
	_, de := s.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &prefix,
	})
	if de != nil {
		logger.Info(module, "delete object r2://%s/%s failed with %s", bucket, prefix, de)
		common.Exit()
	}
	logger.Info(module, "Removing bucket[%s] prefix[%s]", bucket, prefix)
}

// CopyObject copies an object
func (s *S3) Copy(srcBucket, srcPrefix, dstBucket, dstPrefix string) {
	s.init(srcBucket)
	// check object
	if s.S3Attrs(srcBucket, srcPrefix) == nil {
		logger.Debug(module, "failed with bucket[%s] prefix[%s] not an object", srcBucket, srcPrefix)
		return
	}

	_, ce := s.client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(dstBucket),
		Key:        aws.String(dstPrefix),
		CopySource: aws.String(fmt.Sprintf("%v/%v", srcBucket, srcPrefix)),
	})

	if ce != nil {
		logger.Info(module, "copy object failed with %s", ce)
		common.Exit()
	}
	logger.Info(
		module,
		"Copying from bucket[%s] prefix[%s] to bucket[%s] prefix[%s]",
		srcBucket, srcPrefix, dstBucket, dstPrefix,
	)
}

// DownloadObjectWithWorkerPool downloads a specific byte range of an object to a file.
func (s *S3) Download(
	bucket, prefix, dstFile string,
	forceChecksum bool,
	ctx system.RunContext,
) {
	s.init(bucket)
	// check object
	attrs := s.S3Attrs(bucket, prefix)
	if attrs == nil {
		logger.Debug(module, "failed with bucket[%s] prefix[%s] not an object", bucket, prefix)
		return
	}
	chunkSize := int64(googleapi.DefaultUploadChunkSize)
	chunkNumber := int(math.Ceil(float64(attrs.S3Attrs.ObjectSize) / float64(chunkSize)))
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
			length = attrs.S3Attrs.ObjectSize - startByte
		}

		wg.Add(1)
		ctx.Pool.Add(
			func() {
				defer wg.Done()

				// create folder and temp file if not exist
				once.Do(func() {
					pb = ctx.Bars.New(attrs.S3Attrs.ObjectSize, fmt.Sprintf("Downloading [%s]:", prefix))
					folder, _ := common.ParseFile(dstFile)
					if !common.IsPathExist(folder) {
						common.CreateFolder(folder)
					}
					common.CreateFile(dstFileTemp, attrs.S3Attrs.ObjectSize)
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
			},
		)
	}

	// move back the temp file
	ctx.Pool.Add(func() {
		wg.Wait()
		err := os.Rename(dstFileTemp, dstFile)
		if err != nil {
			logger.Info(module, "download object failed when rename file with %s", err)
			common.Exit()
		}
		common.SetFileModificationTime(dstFile, getR2ModificationTime(attrs))
	})
}

// UploadObject uploads an object from a file
func (s *S3) Upload(srcFile, bucket, prefix string, ctx system.RunContext) {
	s.init(bucket)
	// open source file
	f, err := os.Open(srcFile)
	if err != nil {
		logger.Info(module, "upload object failed when open file with %s", err)
		common.Exit()
	}
	defer func() { _ = f.Close() }()

	// progress bar
	//modTime := common.GetFileModificationTime(srcFile)
	logger.Info(module, "uploading %s to %s/%s", srcFile, bucket, prefix)
	// upload file
	_, ue := s.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(prefix),
		Body:   f,
	})
	if ue != nil {
		logger.Info(module, "upload object failed when copy file with %s", ue)
		common.Exit()
	}
}

// MoveObject moves an object
func (s *S3) Move(srcBucket, srcPrefix, dstBucket, dstPrefix string) {
	if srcBucket == dstBucket && srcPrefix == dstPrefix {
		return
	}
	s.Copy(srcBucket, srcPrefix, dstBucket, dstPrefix)
	s.Delete(srcBucket, srcPrefix)
}

// OutputObject outputs an object
func (s *S3) Cat(bucket, prefix string) []byte {
	// create reader
	s.init(bucket)
	o, ge := s.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(prefix),
	})
	if ge != nil {
		logger.Info(module, "output object failed when create reader with %s", ge)
		common.Exit()
	}

	// write to bytes
	buf := new(bytes.Buffer)
	_, re := buf.ReadFrom(o.Body)
	if re != nil {
		logger.Info(module, "output object failed when write to buffer with %s", re)
		common.Exit()
	}
	bs := buf.Bytes()
	return bs
}

// IsObject checks if is an object
// case 1: gs://abc/def -> gs://abc/def/ : false
// case 2: gs://abc/de -> gs://abc/def/ : false
// case 3: gs://abc/def/ -> gs://abc/def/ : false
// case 4: gs://abc/def -> gs://abc/def : true
func (s *S3) IsObject(bucket, prefix string) bool {
	return s.S3Attrs(bucket, prefix) != nil
}

// IsDirectory checks if is a directory
func (s *S3) IsDirectory(bucket, prefix string) bool {
	objs := s.listObjectsAndSubPaths(bucket, prefix, true)
	if len(objs) > 1 {
		return true
	}
	if len(objs) == 1 {
		return len(objs[0]) > len(prefix)
	}
	return false
}

// equalCRC32C return true if CRC32C values are the same
// - compare a local file with an object from gcp
func (s *S3) equalCRC32C(localPath, bucket, object string) bool {
	localCRC32C := common.GetFileCRC32C(localPath)
	r2CRC32C := uint32(0)
	attr := s.S3Attrs(bucket, object)
	if attr != nil {
		r2CRC32C = s.toAttrs(attr).CRC32
	}
	logger.Info(module, "CRC32C checking of local[%s] and bucket[%s] prefix[%s] are [%d] with [%d].",
		localPath, bucket, object, localCRC32C, r2CRC32C)
	return localCRC32C == r2CRC32C
}

// MustEqualCRC32C compare CRC32C values if flag is set
// - compare a local file with an object from gcp
// - exit process if values are different
func (s *S3) MustEqualCRC32C(flag bool, localPath, bucket, object string) {
	if !flag {
		return
	}
	if !s.equalCRC32C(localPath, bucket, object) {
		logger.Info(module, "CRC32C checking failed of local[%s] and bucket[%s] prefix[%s].", localPath, bucket, object)
		common.Exit()
	}
	logger.Info(module, "CRC32C checking success of local[%s] and bucket[%s] prefix[%s].", localPath, bucket, object)
}
