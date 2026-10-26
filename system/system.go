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
	_systems          = map[string]ISystem{}
	ErrObjectNotFound = fmt.Errorf("Object Not Found")
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
	// CalcCRC32C, when set, produces the checksum on demand. The second
	// result says whether it could be determined at all -- a checksum the
	// object never carried, or one a request failed to fetch. A bare uint32
	// could not tell those from a genuine 0, and Same compared the 0 as if it
	// were real, so two objects that both failed to produce a checksum
	// matched on size alone.
	CalcCRC32C func() (uint32, bool)
	// ContentMD5 is the object's MD5 as the service reports it, when a
	// backend has one for free (OCI's listing carries it). For a multipart
	// upload it is the composite form, "<md5 of part md5s>-<parts>", so two
	// equal values mean equal content, while two different ones can still be
	// the same bytes uploaded with another part size. Same therefore only
	// trusts a match, and falls back to the CRC32C when they differ.
	ContentMD5 string
}

// copyResetsModTime is implemented by a backend whose server-side Copy gives
// the destination a new modification time but keeps its content checksums.
type copyResetsModTime interface {
	CopyResetsModTime() bool
}

// CompareByContent reports whether rsync between two locations should compare
// checksums rather than modification times: when asked to (-v), or when both
// ends are a backend whose server-side copy resets the modification time.
// Between two such locations a modification time says only when an object
// was last copied, so comparing it re-copies everything an earlier run copied.
func CompareByContent(src, dst ISystem, forceChecksum bool) bool {
	if forceChecksum {
		return true
	}
	a, ok := src.(copyResetsModTime)
	if !ok || !a.CopyResetsModTime() {
		return false
	}
	b, ok := dst.(copyResetsModTime)
	return ok && b.CopyResetsModTime()
}

func (a *Attrs) Same(b *Attrs, forceChecksum bool) bool {
	if b == nil {
		return false
	}
	r := a.RelativePath == b.RelativePath
	r = r && a.Size == b.Size
	if !forceChecksum && !a.ModTime.Equal(time.Time{}) && !b.ModTime.Equal(time.Time{}) {
		r = r && a.ModTime.Equal(b.ModTime)
	}
	// Nothing below can make two files that already differ match, and
	// CalcCRC32C is the expensive half: on the linux side it reads the whole
	// file, and on s3 it is a request per object. Two files of different sizes
	// were being hashed and fetched only to be declared different anyway.
	if !r {
		return false
	}
	// Equal service-reported MD5s are equal content, and they cost nothing:
	// the listing already carried them. Only a match is trusted -- a
	// multipart MD5 depends on the part size -- and only when content is what
	// is being compared, so the default size-and-mtime check is unchanged.
	if forceChecksum && a.ContentMD5 != "" && a.ContentMD5 == b.ContentMD5 {
		return true
	}
	if a.CalcCRC32C != nil {
		crc, ok := a.CalcCRC32C()
		if !ok {
			return false
		}
		a.CRC32 = crc
	}
	if b.CalcCRC32C != nil {
		crc, ok := b.CalcCRC32C()
		if !ok {
			return false
		}
		b.CRC32 = crc
	}
	return a.CRC32 == b.CRC32
}

type RunContext struct {
	Bars        *bar.Container
	Pool        *worker.Pool
	Concurrency int // workers the pool runs with; 1 unless -m is given
	ChunkSize   int64
	GentleIO    bool
}

type DiskUsage struct {
	Size int64
	Name string
}

type ISystem interface {
	Init(buckets ...string) error
	Scheme() string
	Attributes(bucket, prefix string) (*Attrs, error)
	BatchAttributes(bucket, prefix string, recursive bool) ([]*Attrs, error)
	List(bucket, prefix string, recursive bool) ([]*FileObject, error)
	DiskUsage(bucket, prefix string, recursive bool) ([]DiskUsage, error)
	Delete(bucket, prefix string) error
	Copy(srcBucket, srcPrefix, dstBucket, dstPrefix string) error
	Download(bucket, prefix, dstFile string, forceChecksum bool, ctx RunContext) error
	Upload(srcFile, bucket, object string, ctx RunContext) error
	Move(srcBucket, srcPrefix, dstBucket, dstPrefix string) error
	Cat(bucket, prefix string) ([]byte, error)
	IsObject(bucket, prefix string) (bool, error)
	IsDirectory(bucket, prefix string) (bool, error)
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
	if fo.System == nil {
		return fo.fileType
	}

	// Check if it's a directory
	ok, err := fo.System.IsDirectory(fo.Bucket, fo.Prefix)
	if err != nil {
		common.Exit()
	}
	if ok {
		fo.fileType = FileType_Directory
		return fo.fileType
	}

	// Check if it's an object
	ok, err = fo.System.IsObject(fo.Bucket, fo.Prefix)
	if err != nil {
		common.Exit()
	}
	if ok {
		fo.fileType = FileType_Object
		fo.Attributes, _ = fo.System.Attributes(fo.Bucket, fo.Prefix)
	}

	return fo.fileType
}

func ParseFileObject(path string) *FileObject {
	u, err := url.Parse(path)
	if err != nil {
		logger.Debug("parse", "failed with %s", err)
		return nil
	}
	// from gcs, s3 or oci
	if len(u.Scheme) > 0 {
		system, ok := _systems[u.Scheme]
		if !ok {
			logger.Debug("parse", "invalid scheme: %s", u.Scheme)
			common.Exit()
		}
		if ok {
			// A path may name the authority as user@host. OCI uses that to
			// carry an explicit namespace -- oci://bucket@namespace/x -- and
			// keeping the two joined lets Bucket stay a single opaque string
			// that only the backend concerned has to interpret. It also
			// round-trips: GetFullPath reassembles the path it came from.
			// gs and s3 never produce this form, so they are unaffected.
			bucket := u.Host
			if u.User != nil {
				bucket = u.User.Username() + "@" + u.Host
			}
			fo := &FileObject{
				System: system,
				Bucket: bucket,
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
