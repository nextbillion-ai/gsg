package gcp

import (
	"bytes"
	"context"
	"fmt"
	"gsutil-go/bar"
	"gsutil-go/common"
	"gsutil-go/logger"
	"gsutil-go/worker"
	"io"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	googleApplicationCredentialsEnv = "GOOGLE_APPLICATION_CREDENTIALS"
)

var (
	_client         *storage.Client
	_initClientOnce sync.Once
)

// ConfigPath gets gcp config path from env
func ConfigPath() string {
	return os.Getenv(googleApplicationCredentialsEnv)
}

// storageClient gets or creates a gcp storage client
func storageClient() *storage.Client {
	_initClientOnce.Do(func() {
		path := ConfigPath()
		if path == "" {
			logger.Info("not set environment variable [%s]", googleApplicationCredentialsEnv)
			common.Exit()
		}
		if _, err := os.Stat(path); err != nil {
			logger.Info("failed in loading [%s=%s] with error: %s", googleApplicationCredentialsEnv, path, err)
			common.Exit()
		}
		var err error
		_client, err = storage.NewClient(context.Background(), option.WithCredentialsFile(path))
		if err != nil {
			logger.Debug("failed with %s", err)
		}
	})
	return _client
}

// GetObjectAttributes gets the attributes of an object
func GetObjectAttributes(bucket, prefix string) *storage.ObjectAttrs {
	client := storageClient()
	attrs, err := client.Bucket(bucket).Object(prefix).Attrs(context.Background())
	if err != nil {
		logger.Debug("failed with %s", err)
		return nil
	}
	return attrs
}

// GetObjectsAttributes gets the attributes of all the objects under a prefix
func GetObjectsAttributes(bucket, prefix string, recursive bool) []*storage.ObjectAttrs {
	if !IsObject(bucket, prefix) {
		prefix = common.SetPrefixAsDirectory(prefix)
	}
	res := []*storage.ObjectAttrs{}
	client := storageClient()
	delimiter := "/"
	if recursive {
		delimiter = ""
	}
	it := client.Bucket(bucket).Objects(
		context.Background(),
		&storage.Query{
			Delimiter:  delimiter,
			Prefix:     prefix,
			Projection: storage.ProjectionNoACL,
		},
	)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			logger.Debug("failed with %s", err)
			break
		}
		if len(attrs.Name) > 0 && common.IsSubPath(attrs.Name, prefix) {
			res = append(res, attrs)
		} else if len(attrs.Prefix) > 0 && common.IsSubPath(attrs.Prefix, prefix) {
			res = append(res, attrs)
		}
	}
	return res
}

// ListObjects lists objects under a prefix
func ListObjects(bucket, prefix string, recursive bool) []string {
	res := []string{}
	objs := GetObjectsAttributes(bucket, prefix, recursive)
	for _, obj := range objs {
		if len(obj.Name) > 0 {
			res = append(res, obj.Name)
		} else if len(obj.Prefix) > 0 {
			res = append(res, obj.Prefix)
		}
	}
	return res
}

// GetDiskUsageObjects gets disk usage of objects under a prefix
func GetDiskUsageObjects(bucket, prefix string, recursive bool) []string {
	res := []string{}
	// is object
	obj := GetObjectAttributes(bucket, prefix)
	if obj != nil {
		res = append(res, fmt.Sprintf("%d %s", obj.Size, obj.Name))
		return res
	}
	// is directory
	total := int64(0)
	objs := GetObjectsAttributes(bucket, prefix, recursive)
	for _, obj := range objs {
		if len(obj.Name) > 0 {
			res = append(res, fmt.Sprintf("%d %s", obj.Size, obj.Name))
		} else if len(obj.Prefix) > 0 {
			res = append(res, fmt.Sprintf("%d %s", obj.Size, obj.Prefix))
		}
		total += obj.Size
	}
	if len(res) > 0 {
		res = append(res, fmt.Sprintf("%d %s", total, common.SetPrefixAsDirectory(prefix)))
	}
	return res
}

// DeleteObject deletes an object
func DeleteObject(bucket, prefix string) {
	client := storageClient()
	err := client.Bucket(bucket).Object(prefix).Delete(context.Background())
	if err != nil {
		logger.Debug("failed with %s", err)
	}
	logger.Info("Removing bucket[%s] prefix[%s]", bucket, prefix)
}

// CopyObject copies an object
func CopyObject(srcBucket, srcPrefix, dstBucket, dstPrefix string) {
	// check object
	attrs := GetObjectAttributes(srcBucket, srcPrefix)
	if attrs == nil {
		logger.Debug("failed with bucket[%s] prefix[%s] not an object", srcBucket, srcPrefix)
		return
	}

	// copy object
	client := storageClient()
	src := client.Bucket(srcBucket).Object(srcPrefix)
	dst := client.Bucket(dstBucket).Object(dstPrefix)
	_, err := dst.CopierFrom(src).Run(context.Background())
	if err != nil {
		logger.Debug("failed with %s", err)
	}
	logger.Info(
		"Copying from bucket[%s] prefix[%s] to bucket[%s] prefix[%s]",
		srcBucket, srcPrefix, dstBucket, dstPrefix,
	)

}

// DownloadObjectWithWorkerPool downloads a specific byte range of an object to a file.
func DownloadObjectWithWorkerPool(bucket, prefix, dstFile string, pool *worker.Pool, bars *bar.Container) {
	// check object
	attrs := GetObjectAttributes(bucket, prefix)
	if attrs == nil {
		logger.Debug("failed with bucket[%s] prefix[%s] not an object", bucket, prefix)
		return
	}

	// get chunck size and chunk number
	chunkSize := int64(googleapi.DefaultUploadChunkSize)
	chunkNumber := int(math.Ceil(float64(attrs.Size) / float64(chunkSize)))
	if chunkNumber <= 0 {
		chunkNumber = 1
	}

	// paralell copy by range
	var pb *bar.ProgressBar
	var wg sync.WaitGroup
	var once sync.Once
	client := storageClient()
	dstFileTemp := common.GetTempFile(dstFile)
	for i := 0; i < chunkNumber; i++ {

		// decide offset and length
		startByte := int64(i) * chunkSize
		length := chunkSize
		if i == chunkNumber-1 {
			length = attrs.Size - startByte
		}

		wg.Add(1)
		pool.Add(
			func() {
				defer wg.Done()

				// create folder and temp file if not exist
				once.Do(func() {
					pb = bars.New(attrs.Size, fmt.Sprintf("Downloading [%s]:", prefix))
					folder, _ := common.ParseFile(dstFile)
					if !common.IsPathExist(folder) {
						common.CreateFolder(folder)
					}
					common.CreateFile(dstFileTemp, attrs.Size)
				})

				// create reader with offset and length of object
				rc, err := client.Bucket(bucket).Object(prefix).NewRangeReader(
					context.Background(), startByte, length,
				)
				if err != nil {
					logger.Debug("failed with %s", err)
					return
				}
				defer func() { _ = rc.Close() }()

				// create write with offset and length of file
				fl, err := os.OpenFile(dstFileTemp, os.O_WRONLY, 0766)
				_, err = fl.Seek(startByte, 0)
				if err != nil {
					logger.Debug("failed with %s", err)
					return
				}
				defer func() { _ = fl.Close() }()

				// write data with offset and length to file
				if _, err := io.Copy(io.MultiWriter(fl, pb), rc); err != nil {
					logger.Debug("failed with %s", err)
					return
				}
			},
		)
	}

	// move back the temp file
	pool.Add(func() {
		wg.Wait()
		err := os.Rename(dstFileTemp, dstFile)
		if err != nil {
			logger.Debug("failed with %s", err)
			return
		}
		common.SetFileModificationTime(dstFile, GetFileModificationTime(attrs))
	})
}

// DownloadObject downloads an object to a file
func DownloadObject(bucket, prefix, dstFile string, bars *bar.Container) {
	// check object
	attrs := GetObjectAttributes(bucket, prefix)
	if attrs == nil {
		logger.Debug("failed with bucket[%s] prefix[%s] not an object", bucket, prefix)
		return
	}

	// create target folder
	folder, _ := common.ParseFile(dstFile)
	if !common.IsPathExist(folder) {
		common.CreateFolder(folder)
	}

	// create temp file
	dstFileTemp := common.GetTempFile(dstFile)
	f, err := os.Create(dstFileTemp)
	if err != nil {
		logger.Debug("failed with %s", err)
		return
	}

	// create reader
	client := storageClient()
	rc, err := client.Bucket(bucket).Object(prefix).NewReader(context.Background())
	if err != nil {
		logger.Debug("failed with %s", err)
		return
	}
	defer func() { _ = rc.Close() }()

	// write to file
	pb := bars.New(attrs.Size, fmt.Sprintf("Downloading [%s]:", prefix))
	if _, err := io.Copy(io.MultiWriter(f, pb), rc); err != nil {
		logger.Debug("failed with %s", err)
		return
	}

	// move back temp file
	err = os.Rename(dstFileTemp, dstFile)
	if err != nil {
		logger.Debug("failed with %s", err)
	}
	common.SetFileModificationTime(dstFile, GetFileModificationTime(attrs))
}

// UploadObject uploads an object from a file
func UploadObject(srcFile, bucket, object string, bars *bar.Container) {
	// open source file
	f, err := os.Open(srcFile)
	if err != nil {
		logger.Debug("failed with %s", err)
		return
	}
	defer func() { _ = f.Close() }()

	// progress bar
	size := common.GetFileSize(srcFile)
	pb := bars.New(size, fmt.Sprintf("Uploading [%s]:", srcFile))

	// upload file
	client := storageClient()
	o := client.Bucket(bucket).Object(object)
	wc := o.NewWriter(context.Background())
	if _, err = io.Copy(io.MultiWriter(wc, pb), f); err != nil {
		logger.Debug("failed with %s", err)
		return
	}
	defer func() { _ = wc.Close() }()
}

// MoveObject moves an object
func MoveObject(srcBucket, srcPrefix, dstBucket, dstPrefix string) {
	if srcBucket == dstBucket && srcPrefix == dstPrefix {
		return
	}
	CopyObject(srcBucket, srcPrefix, dstBucket, dstPrefix)
	DeleteObject(srcBucket, srcPrefix)
}

// OutputObject outputs an object
func OutputObject(bucket, prefix string) []byte {
	// create reader
	client := storageClient()
	rc, err := client.Bucket(bucket).Object(prefix).NewReader(context.Background())
	if err != nil {
		logger.Debug("failed with %s", err)
		return nil
	}
	defer func() { _ = rc.Close() }()

	// write to bytes
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(rc)
	if err != nil {
		logger.Debug("failed with %s", err)
		return nil
	}
	bs := buf.Bytes()
	return bs
}

// IsObject checks if is an object
// case 1: gs://abc/def -> gs://abc/def/ : false
// case 2: gs://abc/de -> gs://abc/def/ : false
// case 3: gs://abc/def/ -> gs://abc/def/ : false
// case 4: gs://abc/def -> gs://abc/def : true
func IsObject(bucket, prefix string) bool {
	if GetObjectAttributes(bucket, prefix) != nil {
		return true
	}
	return false
}

// IsDirectory checks if is a directory
// case 1: gs://abc/def -> gs://abc/def/ : true
// case 2: gs://abc/de -> gs://abc/def/ : false
// case 3: gs://abc/def/ -> gs://abc/def/ : true
// case 4: gs://abc/def -> gs://abc/def : false
func IsDirectory(bucket, prefix string) bool {
	objs := GetObjectsAttributes(bucket, prefix, false)
	if len(objs) == 1 {
		if len(objs[0].Name) > len(prefix) {
			return true
		} else if len(objs[0].Prefix) > len(prefix) {
			return true
		}
	} else if len(objs) > 1 {
		return true
	}
	return false
}

// ParseFileModificationTimeMetadata parsed reserved modification time from metadata
func ParseFileModificationTimeMetadata(attrs *storage.ObjectAttrs) time.Time {
	if v, ok := attrs.Metadata["goog-reserved-file-mtime"]; ok {
		if len(v) > 0 {
			ts, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return time.Time{}
			}
			return time.Unix(ts, 0)
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
