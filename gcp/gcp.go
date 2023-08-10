package gcp

import (
	"bytes"
	"context"
	"encoding/binary"
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
			logger.Info("get client failed with %s", err)
			common.Exit()
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
			logger.Info("get objects attributes failed with %s", err)
			common.Exit()
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
		logger.Info("delete object failed with %s", err)
		common.Exit()
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
		logger.Info("copy object failed with %s", err)
		common.Exit()
	}
	logger.Info(
		"Copying from bucket[%s] prefix[%s] to bucket[%s] prefix[%s]",
		srcBucket, srcPrefix, dstBucket, dstPrefix,
	)

}

// DownloadObjectWithWorkerPool downloads a specific byte range of an object to a file.
func DownloadObjectWithWorkerPool(
	bucket, prefix, dstFile string,
	pool *worker.Pool,
	bars *bar.Container,
	forceChecksum bool,
) {
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
					logger.Info("download object failed when create reader with %s", err)
					common.Exit()
				}
				defer func() { _ = rc.Close() }()

				// create write with offset and length of file
				fl, _ := os.OpenFile(dstFileTemp, os.O_WRONLY, 0766)
				_, err = fl.Seek(startByte, 0)
				if err != nil {
					logger.Info("download object failed when seek for offset with %s", err)
					common.Exit()
				}
				defer func() { _ = fl.Close() }()

				// write data with offset and length to file
				if _, err := io.Copy(io.MultiWriter(fl, pb), rc); err != nil {
					logger.Info("download object failed when write to offet with %s", err)
					common.Exit()
				}
			},
		)
	}

	// move back the temp file
	pool.Add(func() {
		wg.Wait()
		err := os.Rename(dstFileTemp, dstFile)
		if err != nil {
			logger.Info("download object failed when rename file with %s", err)
			common.Exit()
		}
		common.SetFileModificationTime(dstFile, GetFileModificationTime(attrs))
		MustEqualCRC32C(forceChecksum, dstFile, bucket, prefix)
	})
}

// DownloadObject downloads an object to a file
func DownloadObject(
	bucket, prefix, dstFile string,
	bars *bar.Container,
	forceChecksum bool,
) {
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
		logger.Info("download object failed when create file with %s", err)
		common.Exit()
	}

	// create reader
	client := storageClient()
	rc, err := client.Bucket(bucket).Object(prefix).NewReader(context.Background())
	if err != nil {
		logger.Info("download object failed with when create reader %s", err)
		common.Exit()
	}
	defer func() { _ = rc.Close() }()

	// write to file
	pb := bars.New(attrs.Size, fmt.Sprintf("Downloading [%s]:", prefix))
	if _, err := io.Copy(io.MultiWriter(f, pb), rc); err != nil {
		logger.Info("download object failed when write to file with %s", err)
		common.Exit()
	}

	// move back temp file
	err = os.Rename(dstFileTemp, dstFile)
	if err != nil {
		logger.Info("download object failed when rename file with %s", err)
		common.Exit()
	}
	common.SetFileModificationTime(dstFile, GetFileModificationTime(attrs))
	MustEqualCRC32C(forceChecksum, dstFile, bucket, prefix)
}

// DoAttemptUnlock takes generation as input and returns potential error
func DoAttemptUnlock(bucket, object string, generation int64) error {
	client := storageClient()
	o := client.Bucket(bucket).Object(object)
	//delete fails means other client has acquired lock
	logger.Debug("DoAttemptUnlock: unlock with generation:%d", generation)
	return o.If(storage.Conditions{GenerationMatch: int64(generation)}).Delete(context.Background())
}

// AttemptUnLock attempts to release a remote lock file
func AttemptUnLock(bucket, object string) {
	cacheFileName := common.GenTempFileName(bucket, "/", object)
	generationBytes, e := os.ReadFile(cacheFileName)
	if e != nil {
		logger.Debug("failed to read lock cache: %+v", cacheFileName)
		common.Finish()
	}
	generation := binary.LittleEndian.Uint64(generationBytes)
	if e := DoAttemptUnlock(bucket, object, int64(generation)); e != nil {
		logger.Debug("unlock error: %+v", e)
		common.Finish()
	}
}

// DoAttemptLock returns generation and potential error
func DoAttemptLock(bucket, object string, ttl time.Duration) (int64, error) {
	// write lock
	client := storageClient()
	o := client.Bucket(bucket).Object(object)
	wc := o.If(storage.Conditions{DoesNotExist: true}).NewWriter(context.Background())
	wc.Write([]byte("1"))
	e0 := wc.Close()
	attrs, e1 := o.Attrs(context.Background())
	if e1 != nil {
		return 0, e1
	}
	if e0 != nil {
		//logger.Debug("DoAttemptLock expire: %+v, current: %+v, ttl:%+v", attrs.Updated, time.Now(), ttl)
		if attrs.Updated.Add(ttl).Before(time.Now()) {
			//logger.Debug("DoAttemptLock expired. delete and try lock again")
			_ = o.If(storage.Conditions{GenerationMatch: attrs.Generation}).Delete(context.Background())
			//try acquire lock again
			wc = o.If(storage.Conditions{DoesNotExist: true}).NewWriter(context.Background())
			wc.Write([]byte("1"))
			if e2 := wc.Close(); e2 != nil {
				return 0, e2
			}
		} else {
			//lock acquire failure, quit with error
			return 0, e0
		}
	}
	//upon sucessful write, store generation in /tmp
	//logger.Debug("DoAttemptLock lock acquired. updating ttl")
	return attrs.Generation, nil

}

// AttemptLock attempts to write a remote lock file
func AttemptLock(bucket, object string, ttl time.Duration) {
	generation, e := DoAttemptLock(bucket, object, ttl)
	if e != nil {
		logger.Info("attemp lock failed: %s", e)
		common.Exit()
	}

	//upon sucessful write, store generation in /tmp
	logger.Debug("AttemptLock: storing generation: %+v", generation)
	cacheFileName := common.GenTempFileName(bucket, "/", object)
	generationBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(generationBytes, uint64(generation))
	if e1 := os.WriteFile(cacheFileName, generationBytes, os.ModePerm); e1 != nil {
		logger.Info("AttemptLock: cache lock generation failed: %s", e1)
		common.Exit()
	}
}

// UploadObject uploads an object from a file
func UploadObject(srcFile, bucket, object string, bars *bar.Container) {
	// open source file
	f, err := os.Open(srcFile)
	if err != nil {
		logger.Info("upload object failed when open file with %s", err)
		common.Exit()
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
		logger.Info("upload object failed when copy file with %s", err)
		common.Exit()
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
		logger.Info("output object failed when create reader with %s", err)
		common.Exit()
	}
	defer func() { _ = rc.Close() }()

	// write to bytes
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(rc)
	if err != nil {
		logger.Info("output object failed when write to buffer with %s", err)
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

// equalCRC32C return true if CRC32C values are the same
// - compare a local file with an object from gcp
func equalCRC32C(localPath, bucket, object string) bool {
	localCRC32C := common.GetFileCRC32C(localPath)
	gcpCRC32C := uint32(0)
	attr := GetObjectAttributes(bucket, object)
	if attr != nil {
		gcpCRC32C = attr.CRC32C
	}
	logger.Info("CRC32C checking of local[%s] and bucket[%s] prefix[%s] are [%d] with [%d].",
		localPath, bucket, object, localCRC32C, gcpCRC32C)
	return localCRC32C == gcpCRC32C
}

// MustEqualCRC32C compare CRC32C values if flag is set
// - compare a local file with an object from gcp
// - exit process if values are different
func MustEqualCRC32C(flag bool, localPath, bucket, object string) {
	if !flag {
		return
	}
	if !equalCRC32C(localPath, bucket, object) {
		logger.Info("CRC32C checking failed of local[%s] and bucket[%s] prefix[%s].", localPath, bucket, object)
		common.Exit()
	}
	logger.Info("CRC32C checking success of local[%s] and bucket[%s] prefix[%s].", localPath, bucket, object)
}
