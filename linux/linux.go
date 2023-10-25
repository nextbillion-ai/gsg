package linux

import (
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/nextbillion-ai/gsg/system"
)

const (
	module = "LINUX"
)

var (
	whiteSpaces = regexp.MustCompile(`[\s]+`)
)

// FileAttrs holds attributes of a file
type FileAttrs struct {
	FullPath     string
	RelativePath string
	Name         string
	Size         int64
	CRC32C       uint32
	ModTime      time.Time
}

// Same check if two attributes are same file storing differently
// - refer to gsutil logic:
// - - https://cloud.google.com/storage/docs/gsutil/commands/rsync
func (fa *FileAttrs) Same(other *FileAttrs, forceChecksum bool) bool {
	if other == nil {
		return false
	}

	// compare file name
	if fa.RelativePath != other.RelativePath {
		return false
	}

	// compare file size
	if fa.Size != other.Size {
		return false
	}

	if !forceChecksum {
		// compare modification time
		if !fa.ModTime.Equal(time.Time{}) && !other.ModTime.Equal(time.Time{}) {
			return fa.ModTime.Equal(other.ModTime)
		}
	}

	// compare crc32
	if fa.CRC32C <= 0 {
		fa.CRC32C = common.GetFileCRC32C(fa.FullPath)
	}
	if other.CRC32C <= 0 {
		other.CRC32C = common.GetFileCRC32C(other.FullPath)
	}
	logger.Info(module, "CRC32C checking of [%s] and [%s] are [%d] with [%d].", fa.FullPath, other.FullPath, fa.CRC32C, other.CRC32C)
	return fa.CRC32C == other.CRC32C
}

// GetRealPath gets real path of a directory
func GetRealPath(dir string) string {
	r, e := filepath.Abs(dir)
	if e != nil {
		logger.Debug(module, "GetRealPath failed:  %s", e)
		return ""
	}
	return r
}

type Linux struct {
}

func (l *Linux) Scheme() string {
	return ""
}

func (l *Linux) toAttrs(attrs *FileAttrs) *system.Attrs {
	if attrs == nil {
		return nil
	}
	return &system.Attrs{
		Size:    attrs.Size,
		CRC32:   attrs.CRC32C,
		ModTime: attrs.ModTime,
	}
}

func (l *Linux) toFileObject(path string) *system.FileObject {
	path = GetRealPath(path)
	fo := &system.FileObject{
		System: l,
		Bucket: "",
		Prefix: path,
		Remote: false,
	}
	fo.SetAttributes(l.toAttrs(l.attrs("", path)))
	return fo
}
func (l *Linux) attrs(bucket, prefix string) *FileAttrs {
	if !common.IsPathExist(prefix) {
		return nil
	}
	_, name := common.ParseFile(prefix)
	res := &FileAttrs{
		FullPath:     prefix,
		RelativePath: prefix,
		Name:         name,
		Size:         common.GetFileSize(prefix),
		CRC32C:       common.GetFileCRC32C(prefix),
		ModTime:      common.GetFileModificationTime(prefix),
	}
	return res
}

func (l *Linux) Attributes(bucket, prefix string) *system.Attrs {
	return l.toAttrs(l.attrs(bucket, prefix))
}

// GetObjectsAttributes gets attributes of all the files under a dir
func (l *Linux) batchAttrs(bucket, prefix string, isRec bool) []*FileAttrs {
	res := []*FileAttrs{}
	dir := GetRealPath(prefix)
	objs := l.List(bucket, dir, isRec)
	for _, obj := range objs {
		_, name := common.ParseFile(obj.Prefix)
		res = append(res, &FileAttrs{
			FullPath:     obj.Prefix,
			RelativePath: common.GetRelativePath(dir, obj.Prefix),
			Name:         name,
			Size:         common.GetFileSize(obj.Prefix),
			CRC32C:       0, // low performance so only set default value, populate when necessary
			ModTime:      common.GetFileModificationTime(obj.Prefix),
		})
	}
	return res
}

func (l *Linux) BatchAttributes(bucket, prefix string, recursive bool) []*system.Attrs {
	res := []*system.Attrs{}
	for _, attr := range l.batchAttrs(bucket, prefix, recursive) {
		res = append(res, l.toAttrs(attr))
	}
	return res

}

// ListObjects lists objects under a prefix
func (l *Linux) List(bucket, prefix string, isRec bool) []*system.FileObject {
	dir := GetRealPath(prefix)
	var stdout []byte
	var err error
	if isRec {
		stdout, err = exec.Command("find", dir, "-type", "f").Output()
	} else {
		stdout, err = exec.Command("find", dir, "-type", "f", "-maxdepth", "1").Output()
	}
	if err != nil {
		logger.Debug(module, "failed with %s", err)
		return []*system.FileObject{}
	}
	res := strings.Split(string(stdout), "\n")
	objs := []*system.FileObject{}
	for _, v := range res {
		v = strings.Trim(v, " \t\n")
		if len(v) > 0 && !common.IsTempFile(v) {
			objs = append(objs, l.toFileObject(v))
		}
	}
	return objs
}

// ListTempFiles lists objects under a prefix
func ListTempFiles(dir string, isRec bool) []string {
	dir = GetRealPath(dir)
	var stdout []byte
	var err error
	if isRec {
		stdout, err = exec.Command("find", dir, "-type", "f").Output()
	} else {
		stdout, err = exec.Command("find", dir, "-type", "f", "-maxdepth", "1").Output()
	}
	if err != nil {
		logger.Debug(module, "failed with %s", err)
		return []string{}
	}
	res := strings.Split(string(stdout), "\n")
	objs := []string{}
	for _, v := range res {
		v = strings.Trim(v, " \t\n")
		if len(v) > 0 && common.IsTempFile(v) {
			objs = append(objs, v)
		}
	}
	return objs
}

// GetDiskUsageObjects gets disk usage of objects under a prefix
func (l *Linux) DiskUsage(bucket, prefix string, recursive bool) []system.DiskUsage {
	dir := GetRealPath(prefix)
	objs := []system.DiskUsage{}
	stdout, err := exec.Command("du", "-aB1", dir).Output()
	if err != nil {
		logger.Debug(module, "failed with %s", err)
		return objs
	}
	res := strings.Split(string(stdout), "\n")
	for _, v := range res {
		v = strings.Trim(v, " \t\n")
		if len(v) > 0 {
			items := whiteSpaces.Split(v, 2)
			logger.Debug(module, "%s,%s", items[0], items[1])
			size, err := strconv.ParseInt(items[0], 10, 64)
			if err != nil {
				continue
			}
			objs = append(objs, system.DiskUsage{Size: size, Name: items[1]})
		}
	}
	return objs
}

func (l *Linux) Download(
	bucket, prefix, dstFile string,
	forceChecksum bool,
	ctx system.RunContext,
) {
	panic("Linux::Download should not be involked!")
}

func (l *Linux) Upload(srcFile, bucket, object string, ctx system.RunContext) {
	panic("Linux::Upload should not be involked!")
}

// DeleteObject deletes an object
func (l *Linux) Delete(bucket, prefix string) {
	_, err := exec.Command("rm", "-rf", prefix).Output()
	if err != nil {
		logger.Debug(module, "failed with %s", err)
	}
	logger.Info(module, "Removing path[%s]", prefix)
}

// CopyObject copies an object
func (l *Linux) Copy(srcBucket, srcPath, dstBucket, dstPath string) {
	folder, _ := common.ParseFile(dstPath)
	if !common.IsPathExist(folder) {
		common.CreateFolder(folder)
	}
	_, err := exec.Command("cp", "-rf", srcPath, dstPath).Output()
	if err != nil {
		logger.Debug(module, "failed with %s", err)
	}
	logger.Info(module, "Copying from path[%s] to path[%s]", srcPath, dstPath)
}

// MoveObject moves an object
func (l *Linux) Move(srcBucket, srcPath, dstBucket, dstPath string) {
	_, err := exec.Command("mv", srcPath, dstPath).Output()
	if err != nil {
		logger.Debug(module, "failed with %s", err)
	}
	logger.Info(module, "Moving from path[%s] to path[%s]", srcPath, dstPath)
}

// OutputObject outputs an object
func (l *Linux) Cat(bucket, path string) []byte {
	bs, err := exec.Command("cat", path).Output()
	if err != nil {
		logger.Info(module, "failed with %s", err)
		return nil
	}
	return bs
}

// IsDirectoryOrObject checks if is a directory or an object
func IsDirectoryOrObject(path string) bool {
	return common.IsPathDirectory(path) || common.IsPathFile(path)
}

// IsObject checks if is an object
func (l *Linux) IsObject(bucket, path string) bool {
	return common.IsPathFile(path)
}

// IsDirectory checks if is a directory
func (l *Linux) IsDirectory(bucket, path string) bool {
	return common.IsPathDirectory(path)
}
