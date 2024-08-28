package gcs

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
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
)

const (
	googleApplicationCredentialsEnv = "GOOGLE_APPLICATION_CREDENTIALS"
	module                          = "GCS"
)

// ConfigPath gets gcp config path from env
func ConfigPath() string {
	return os.Getenv(googleApplicationCredentialsEnv)
}

type GCS struct {
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
	g.client, err = storage.NewClient(context.Background(), option.WithCredentialsFile(path))
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
		var du *system.DUTree
		if len(obj.Name) > 0 {
			du = system.NewDUTree(obj.Name, obj.Size, false)
		} else if len(obj.Prefix) > 0 {
			du = system.NewDUTree(obj.Prefix, obj.Size, false)
		}
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
	chunkSize := int64(googleapi.DefaultUploadChunkSize)
	chunkNumber := int(math.Ceil(float64(attrs.Size) / float64(chunkSize)))
	if chunkNumber <= 0 {
		chunkNumber = 1
	}

	// paralell copy by range
	var pb *bar.ProgressBar
	var wg sync.WaitGroup
	var once sync.Once
	dstFileTemp := common.GetTempFile(dstFile)
	for i := 0; i < chunkNumber; i++ {

		// decide offset and length
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
				rc, err := g.client.Bucket(bucket).Object(prefix).NewRangeReader(
					context.Background(), startByte, length,
				)
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

				// write data with offset and length to file
				if _, err = io.Copy(io.MultiWriter(fl, pb), rc); err != nil {
					logger.Info(module, "download object failed when write to offet with %s", err)
					common.Exit()
				}
				if err = fl.Sync(); err != nil {
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
	common.SetFileModificationTime(dstFile, GetFileModificationTime(attrs))
	if err = g.MustEqualCRC32C(forceChecksum, dstFile, bucket, prefix); err != nil {
		return err
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
	generationBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(generationBytes, uint64(generation))
	if e1 := os.WriteFile(cacheFileName, generationBytes, os.ModePerm); e1 != nil {
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
	if f, err = os.Open(srcFile); err != nil {
		logger.Info(module, "upload object failed when open file with %s", err)
		return err
	}
	defer func() { _ = f.Close() }()

	// progress bar
	size := common.GetFileSize(srcFile)
	modTime := common.GetFileModificationTime(srcFile)
	pb := ctx.Bars.New(size, fmt.Sprintf("Uploading [%s]:", srcFile))

	// upload file
	o := g.client.Bucket(bucket).Object(object)
	wc := o.NewWriter(context.Background())
	wc.Metadata = map[string]string{
		"goog-reserved-file-mtime": strconv.FormatInt(modTime.UnixNano(), 10),
	}
	if _, err = io.Copy(io.MultiWriter(wc, pb), f); err != nil {
		logger.Info(module, "upload object failed when copy file with %s", err)
		return err
	}
	defer func() { _ = wc.Close() }()
	return nil
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

// IsDirectory checks if is a directory
// case 1: gs://abc/def -> gs://abc/def/ : true
// case 2: gs://abc/de -> gs://abc/def/ : false
// case 3: gs://abc/def/ -> gs://abc/def/ : true
// case 4: gs://abc/def -> gs://abc/def : false
func (g *GCS) IsDirectory(bucket, prefix string) (bool, error) {
	var err error
	var objs []*storage.ObjectAttrs
	if objs, err = g.batchAttrs(bucket, prefix, false); err != nil {
		return false, err
	}
	if len(objs) == 1 {
		if len(objs[0].Name) > len(prefix) {
			return true, nil
		} else if len(objs[0].Prefix) > len(prefix) {
			return true, nil
		}
	} else if len(objs) > 1 {
		return true, nil
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

// equalCRC32C return true if CRC32C values are the same
// - compare a local file with an object from gcp
func (g *GCS) equalCRC32C(localPath, bucket, object string) (bool, error) {
	localCRC32C := common.GetFileCRC32C(localPath)
	gcpCRC32C := uint32(0)
	var err error
	var attr *storage.ObjectAttrs
	if attr, err = g.GCSAttrs(bucket, object); err != nil {
		return false, err
	}
	if attr != nil {
		gcpCRC32C = attr.CRC32C
	}
	logger.Info(module, "CRC32C checking of local[%s] and bucket[%s] prefix[%s] are [%d] with [%d].",
		localPath, bucket, object, localCRC32C, gcpCRC32C)
	return localCRC32C == gcpCRC32C, nil
}

// MustEqualCRC32C compare CRC32C values if flag is set
// - compare a local file with an object from gcp
// - exit process if values are different
func (g *GCS) MustEqualCRC32C(flag bool, localPath, bucket, object string) error {
	if !flag {
		return nil
	}
	var err error
	var ok bool
	if ok, err = g.equalCRC32C(localPath, bucket, object); err != nil {
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
