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

func (l *Linux) Init(_ ...string) error {
	return nil
}

func (l *Linux) attrs(_, prefix string) *FileAttrs {
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

func (l *Linux) Attributes(bucket, prefix string) (*system.Attrs, error) {
	return l.toAttrs(l.attrs(bucket, prefix)), nil
}

// GetObjectsAttributes gets attributes of all the files under a dir
func (l *Linux) batchAttrs(bucket, prefix string, isRec bool) ([]*FileAttrs, error) {
	res := []*FileAttrs{}
	dir := GetRealPath(prefix)
	var err error
	var objs []*system.FileObject

	if objs, err = l.List(bucket, dir, isRec); err != nil {
		return nil, err
	}
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
	return res, nil
}

func (l *Linux) BatchAttributes(bucket, prefix string, recursive bool) ([]*system.Attrs, error) {
	res := []*system.Attrs{}
	var err error
	var fas []*FileAttrs
	if fas, err = l.batchAttrs(bucket, prefix, recursive); err != nil {
		return nil, err
	}

	for _, attr := range fas {
		res = append(res, l.toAttrs(attr))
	}
	return res, nil

}

// ListObjects lists objects under a prefix
func (l *Linux) List(bucket, prefix string, isRec bool) ([]*system.FileObject, error) {
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
		return nil, nil
	}
	res := strings.Split(string(stdout), "\n")
	objs := []*system.FileObject{}
	for i, v := range res {
		if i%100000 == 0 && i != 0 {
			logger.Info(module, "ListObjects %d/%d", i, len(res))
		}
		v = strings.Trim(v, " \t\n")

		if len(v) > 0 && !common.IsTempFile(v) {
			objs = append(objs, l.toFileObject(v))
		}
	}
	return objs, nil
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
		return nil
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
func (l *Linux) DiskUsage(bucket, prefix string, recursive bool) ([]system.DiskUsage, error) {
	dir := GetRealPath(prefix)
	objs := []system.DiskUsage{}
	stdout, err := exec.Command("du", "-aB1", dir).Output()
	if err != nil {
		logger.Debug(module, "failed with %s", err)
		return nil, err
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
	return objs, nil
}

func (l *Linux) Download(
	bucket, prefix, dstFile string,
	forceChecksum bool,
	ctx system.RunContext,
) error {
	panic("Linux::Download should not be involked!")
}

func (l *Linux) Upload(srcFile, bucket, object string, ctx system.RunContext) error {
	panic("Linux::Upload should not be involked!")
}

// DeleteObject deletes an object
func (l *Linux) Delete(bucket, prefix string) error {
	var err error
	if _, err = exec.Command("rm", "-rf", prefix).Output(); err != nil {
		logger.Debug(module, "failed with %s", err)
		return err
	}
	logger.Info(module, "Removing path[%s]", prefix)
	return nil
}

// CopyObject copies an object
func (l *Linux) Copy(srcBucket, srcPath, dstBucket, dstPath string) error {
	folder, _ := common.ParseFile(dstPath)
	if !common.IsPathExist(folder) {
		common.CreateFolder(folder)
	}
	var err error
	if _, err = exec.Command("cp", "-rf", srcPath, dstPath).Output(); err != nil {
		logger.Debug(module, "failed with %s", err)
		return err
	}
	logger.Info(module, "Copying from path[%s] to path[%s]", srcPath, dstPath)
	return nil
}

// MoveObject moves an object
func (l *Linux) Move(srcBucket, srcPath, dstBucket, dstPath string) error {
	var err error
	if _, err = exec.Command("mv", srcPath, dstPath).Output(); err != nil {
		logger.Debug(module, "failed with %s", err)
		return err
	}
	logger.Info(module, "Moving from path[%s] to path[%s]", srcPath, dstPath)
	return nil
}

// OutputObject outputs an object
func (l *Linux) Cat(bucket, path string) ([]byte, error) {
	var err error
	var bs []byte
	if bs, err = exec.Command("cat", path).Output(); err != nil {
		logger.Info(module, "failed with %s", err)
		return nil, err
	}
	return bs, nil
}

// IsDirectoryOrObject checks if is a directory or an object
func IsDirectoryOrObject(path string) bool {
	return common.IsPathDirectory(path) || common.IsPathFile(path)
}

// IsObject checks if is an object
func (l *Linux) IsObject(bucket, path string) (bool, error) {
	return common.IsPathFile(path), nil
}

// IsDirectory checks if is a directory
func (l *Linux) IsDirectory(bucket, path string) (bool, error) {
	return common.IsPathDirectory(path), nil
}
