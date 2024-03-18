package system

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/nextbillion-ai/gsg/bar"
	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/worker"
)

const (
	FileType_Invalid int = iota + 1
	FileType_Object
	FileType_Directory
	module = "system"
)

var (
	_systems = map[string]ISystem{}
)

func Register(system ISystem) {
	_systems[system.Scheme()] = system
}

func Lookup(scheme string) ISystem {
	return _systems[scheme]
}

type Attrs struct {
	Size         int64
	CRC32        uint32
	ModTime      time.Time
	RelativePath string
}

func (a *Attrs) Same(b *Attrs, forceChecksum bool) bool {
	if b == nil {
		return false
	}
	var r bool
	r = a.RelativePath == b.RelativePath
	r = r && a.Size == b.Size
	if !forceChecksum && !a.ModTime.Equal(time.Time{}) && !b.ModTime.Equal(time.Time{}) {
		r = r && a.ModTime.Equal(b.ModTime)
	}
	r = r && a.CRC32 == b.CRC32
	return r
}

type RunContext struct {
	Bars *bar.Container
	Pool *worker.Pool
}

type DiskUsage struct {
	Size int64
	Name string
}

type ISystem interface {
	Scheme() string
	Attributes(bucket, prefix string) *Attrs
	BatchAttributes(bucket, prefix string, recursive bool) []*Attrs
	List(bucket, prefix string, recursive bool) []*FileObject
	DiskUsage(bucket, prefix string, recursive bool) []DiskUsage
	Delete(bucket, prefix string)
	Copy(srcBucket, srcPrefix, dstBucket, dstPrefix string)
	Download(bucket, prefix, dstFile string, forceChecksum bool, ctx RunContext)
	Upload(srcFile, bucket, object string, ctx RunContext)
	Move(srcBucket, srcPrefix, dstBucket, dstPrefix string)
	Cat(bucket, prefix string) []byte
	IsObject(bucket, prefix string) bool
	IsDirectory(bucket, prefix string) bool
}

type FileObject struct {
	System     ISystem
	Bucket     string
	Prefix     string
	Remote     bool
	fileType   int
	Attributes *Attrs
}

func (fo *FileObject) GetFullPath() string {
	return fmt.Sprintf("%s://%s/%s", fo.System.Scheme(), fo.Bucket, fo.Prefix)
}

func (fo *FileObject) SetAttributes(attrs *Attrs) {
	if fo.fileType != 0 {
		logger.Error(module, "invalid overwriting fileType from %d to %d", fo.fileType, FileType_Object)
		common.Exit()
	}
	if strings.HasSuffix(fo.Prefix, "/") {
		fo.fileType = FileType_Directory
	} else {
		fo.fileType = FileType_Object
	}
	fo.Attributes = attrs
}

func (fo *FileObject) SetInvalid() {
	if fo.fileType != 0 {
		logger.Error(module, "invalid overwriting fileType from %d to %d", fo.fileType, FileType_Invalid)
		common.Exit()
	}
	fo.fileType = FileType_Invalid
}

func (fo *FileObject) FileType() int {
	if fo.fileType != 0 {
		return fo.fileType
	}
	fo.fileType = FileType_Invalid
	if fo.System != nil {
		if fo.System.IsDirectory(fo.Bucket, fo.Prefix) {
			fo.fileType = FileType_Directory
		} else if fo.System.IsObject(fo.Bucket, fo.Prefix) {
			fo.fileType = FileType_Object
			fo.Attributes = fo.System.Attributes(fo.Bucket, fo.Prefix)
		}
	}
	return fo.fileType
}

func ParseFileObject(path string) *FileObject {
	u, err := url.Parse(path)
	if err != nil {
		logger.Debug("parse", "failed with %s", err)
		return nil
	}
	// from gcs or s3
	if len(u.Scheme) > 0 {
		system, ok := _systems[u.Scheme]
		if !ok {
			logger.Debug("parse", "invalid scheme: %s", u.Scheme)
			common.Exit()
		}
		if ok {
			fo := &FileObject{
				System: system,
				Bucket: u.Host,
				Prefix: strings.TrimLeft(u.Path, "/"),
				Remote: true,
			}
			logger.Debug("parse", "fo: %+v", fo)
			return fo
		}
	}
	l, ok := _systems[""]
	if !ok {
		logger.Debug("parse", "linux system not found")
		common.Exit()
	}
	path, e := filepath.Abs(u.Path)
	if e != nil {
		fo := &FileObject{
			System: l,
			Bucket: "",
			Prefix: u.Path,
			Remote: false,
		}
		fo.SetInvalid()
		return fo
	}
	fo := &FileObject{
		System: l,
		Bucket: "",
		Prefix: path,
		Remote: false,
	}
	return fo
}
