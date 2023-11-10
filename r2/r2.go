package r2

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	R2CredentialsEnv = "R2_CREDENTIALS"
	module           = "R2"
)

type R2Config struct {
	AccountID       string `json:"accountId"`
	AccessKeyID     string `json:"accessKeyId"`
	AccessKeySecret string `json:"accessKeySecret"`
}

type R2 struct {
	client *s3.Client
}

func (g *R2) Scheme() string {
	return "r2"
}

type R2Attributes struct {
	S3Attrs *s3.GetObjectAttributesOutput
	Bucket  string
	Prefix  string
}

func (r *R2) toAttrs(attrs *R2Attributes) *system.Attrs {
	if attrs == nil {
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

func getR2ModificationTime(attrs *R2Attributes) time.Time {
	if attrs.S3Attrs == nil {
		return time.Time{}
	}
	if attrs.S3Attrs.LastModified == nil {
		return time.Time{}
	}
	return *attrs.S3Attrs.LastModified
}

func (r *R2) toFileObject(attrs *R2Attributes) *system.FileObject {
	if attrs == nil {
		return nil
	}
	fo := &system.FileObject{
		System: r,
		Bucket: attrs.Bucket,
		Prefix: attrs.Prefix,
		Remote: true,
	}
	fo.SetAttributes(r.toAttrs(attrs))
	return fo
}

// storageClient gets or creates a gcp storage client
func (r *R2) init() {
	if r.client != nil {
		return
	}
	path := os.Getenv(R2CredentialsEnv)
	if path == "" {
		logger.Info(module, "expected env-var [%s] not found", R2CredentialsEnv)
		common.Exit()
	}
	r2Config := R2Config{}
	var b []byte
	var re, je error
	if b, re = os.ReadFile(path); re == nil {
		je = json.Unmarshal(b, &r2Config)
	}
	if re != nil || je != nil {
		var err error
		if re != nil {
			err = re
		} else {
			err = je
		}
		logger.Info(module, "failed in loading [%s=%s] with error: %s", R2CredentialsEnv, path, err)
		common.Exit()
	}
	r2Resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: fmt.Sprintf("https://%s.r2.cloudflarestorage.com", r2Config.AccountID),
		}, nil
	})

	cfg, e1 := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolverWithOptions(r2Resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(r2Config.AccessKeyID, r2Config.AccessKeySecret, "")),
	)
	if e1 != nil {
		logger.Info(module, "failed in loading defaultConfig with error: %s", e1)
		common.Exit()
	}

	r.client = s3.NewFromConfig(cfg)
}

func (r *R2) R2Attrs(bucket, prefix string) *R2Attributes {
	r.init()
	var oat types.ObjectAttributes
	attrs, err := r.client.GetObjectAttributes(context.TODO(), &s3.GetObjectAttributesInput{
		Bucket:           aws.String(bucket),
		Key:              aws.String(prefix),
		ObjectAttributes: oat.Values(),
	})
	if err != nil {
		logger.Debug(module, "failed with r2://%s/%s %s", bucket, prefix, err)
		return nil
	}
	return &R2Attributes{
		S3Attrs: attrs,
		Bucket:  bucket,
		Prefix:  prefix,
	}
}

// GetObjectAttributes gets the attributes of an object
func (r *R2) Attributes(bucket, prefix string) *system.Attrs {
	return r.toAttrs(r.R2Attrs(bucket, prefix))
}

func (r *R2) batchAttrs(bucket, prefix string, recursive bool) []*R2Attributes {
	r.init()
	if !r.IsObject(bucket, prefix) {
		prefix = common.SetPrefixAsDirectory(prefix)
	}
	li := s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	}
	delimiter := "/"
	if !recursive {
		li.Delimiter = &delimiter
	}
	lo, le := r.client.ListObjectsV2(context.TODO(), &li)
	if le != nil {
		logger.Info(module, "get objects attributes failed with %s", le)
		common.Exit()
	}
	res := []*R2Attributes{}
	for _, o := range lo.Contents {
		res = append(res, r.R2Attrs(bucket, *o.Key))
	}
	return res

}

// GetObjectsAttributes gets the attributes of all the objects under a prefix
func (r *R2) BatchAttributes(bucket, prefix string, recursive bool) []*system.Attrs {
	res := []*system.Attrs{}
	for _, attr := range r.batchAttrs(bucket, prefix, recursive) {
		res = append(res, r.toAttrs(attr))
	}
	return res
}

// List objects under a prefix
func (r *R2) List(bucket, prefix string, recursive bool) []*system.FileObject {
	fos := []*system.FileObject{}
	for _, attr := range r.batchAttrs(bucket, prefix, recursive) {
		fos = append(fos, r.toFileObject(attr))
	}
	return fos
}

// GetDiskUsageObjects gets disk usage of objects under a prefix
func (r *R2) DiskUsage(bucket, prefix string, recursive bool) []system.DiskUsage {
	res := []system.DiskUsage{}
	// is object
	obj := r.R2Attrs(bucket, prefix)
	if obj != nil {
		res = append(res, system.DiskUsage{Size: obj.S3Attrs.ObjectSize, Name: obj.Prefix})
		return res
	}
	// is directory
	total := int64(0)
	objs := r.batchAttrs(bucket, prefix, recursive)
	for _, obj := range objs {
		res = append(res, system.DiskUsage{Size: obj.S3Attrs.ObjectSize, Name: obj.Prefix})
		total += obj.S3Attrs.ObjectSize
	}
	if len(res) > 0 {
		res = append(res, system.DiskUsage{Size: total, Name: common.SetPrefixAsDirectory(prefix)})
	}
	return res
}

// DeleteObject deletes an object
func (r *R2) Delete(bucket, prefix string) {
	r.init()
	_, de := r.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
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
func (r *R2) Copy(srcBucket, srcPrefix, dstBucket, dstPrefix string) {
	r.init()
	// check object
	if r.R2Attrs(srcBucket, srcPrefix) == nil {
		logger.Debug(module, "failed with bucket[%s] prefix[%s] not an object", srcBucket, srcPrefix)
		return
	}

	_, ce := r.client.CopyObject(context.TODO(), &s3.CopyObjectInput{
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
func (r *R2) Download(
	bucket, prefix, dstFile string,
	forceChecksum bool,
	ctx system.RunContext,
) {
	r.init()
	// check object
	attrs := r.R2Attrs(bucket, prefix)
	if attrs == nil {
		logger.Debug(module, "failed with bucket[%s] prefix[%s] not an object", bucket, prefix)
		return
	}
	oo, oe := r.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(prefix),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if oe != nil {
		logger.Info(module, "download object failed when create reader with %s", oe)
		common.Exit()
	}

	var wg sync.WaitGroup
	dstFileTemp := common.GetTempFile(dstFile)

	wg.Add(1)
	ctx.Pool.Add(
		func() {
			defer wg.Done()
			folder, _ := common.ParseFile(dstFile)
			if !common.IsPathExist(folder) {
				common.CreateFolder(folder)
			}
			common.CreateFile(dstFileTemp, oo.ContentLength)
			file, _ := os.OpenFile(dstFileTemp, os.O_WRONLY, 0766)
			defer file.Close()
			_, ce := io.Copy(file, oo.Body)
			if ce != nil {
				logger.Info(module, "io Copy failed with: %s", ce)
				common.Exit()
			}
		},
	)

	// move back the temp file
	ctx.Pool.Add(func() {
		wg.Wait()
		err := os.Rename(dstFileTemp, dstFile)
		if err != nil {
			logger.Info(module, "download object failed when rename file with %s", err)
			common.Exit()
		}
		common.SetFileModificationTime(dstFile, getR2ModificationTime(attrs))
		r.MustEqualCRC32C(forceChecksum, dstFile, bucket, prefix)
	})
}

// UploadObject uploads an object from a file
func (r *R2) Upload(srcFile, bucket, prefix string, ctx system.RunContext) {
	r.init()
	// open source file
	f, err := os.Open(srcFile)
	if err != nil {
		logger.Info(module, "upload object failed when open file with %s", err)
		common.Exit()
	}
	defer func() { _ = f.Close() }()

	// progress bar
	//modTime := common.GetFileModificationTime(srcFile)

	// upload file
	_, ue := r.client.PutObject(context.TODO(), &s3.PutObjectInput{
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
func (r *R2) Move(srcBucket, srcPrefix, dstBucket, dstPrefix string) {
	if srcBucket == dstBucket && srcPrefix == dstPrefix {
		return
	}
	r.Copy(srcBucket, srcPrefix, dstBucket, dstPrefix)
	r.Delete(srcBucket, srcPrefix)
}

// OutputObject outputs an object
func (r *R2) Cat(bucket, prefix string) []byte {
	// create reader
	r.init()
	o, ge := r.client.GetObject(context.TODO(), &s3.GetObjectInput{
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
func (r *R2) IsObject(bucket, prefix string) bool {
	return r.R2Attrs(bucket, prefix) != nil
}

// IsDirectory checks if is a directory
// case 1: gs://abc/def -> gs://abc/def/ : true
// case 2: gs://abc/de -> gs://abc/def/ : false
// case 3: gs://abc/def/ -> gs://abc/def/ : true
// case 4: gs://abc/def -> gs://abc/def : false
func (r *R2) IsDirectory(bucket, prefix string) bool {
	objs := r.batchAttrs(bucket, prefix, false)
	if len(objs) == 1 {
		if len(objs[0].Prefix) > len(prefix) {
			return true
		}
	} else if len(objs) > 1 {
		return true
	}
	return false
}

// equalCRC32C return true if CRC32C values are the same
// - compare a local file with an object from gcp
func (r *R2) equalCRC32C(localPath, bucket, object string) bool {
	localCRC32C := common.GetFileCRC32C(localPath)
	r2CRC32C := uint32(0)
	attr := r.R2Attrs(bucket, object)
	if attr != nil {
		r2CRC32C = r.toAttrs(attr).CRC32
	}
	logger.Info(module, "CRC32C checking of local[%s] and bucket[%s] prefix[%s] are [%d] with [%d].",
		localPath, bucket, object, localCRC32C, r2CRC32C)
	return localCRC32C == r2CRC32C
}

// MustEqualCRC32C compare CRC32C values if flag is set
// - compare a local file with an object from gcp
// - exit process if values are different
func (r *R2) MustEqualCRC32C(flag bool, localPath, bucket, object string) {
	if !flag {
		return
	}
	if !r.equalCRC32C(localPath, bucket, object) {
		logger.Info(module, "CRC32C checking failed of local[%s] and bucket[%s] prefix[%s].", localPath, bucket, object)
		common.Exit()
	}
	logger.Info(module, "CRC32C checking success of local[%s] and bucket[%s] prefix[%s].", localPath, bucket, object)
}