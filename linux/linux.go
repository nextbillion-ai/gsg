package linux

import (
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
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
	logger.Info("CRC32C checking of [%s] and [%s] are [%d] with [%d].", fa.FullPath, other.FullPath, fa.CRC32C, other.CRC32C)
	return fa.CRC32C == other.CRC32C
}

// GetRealPath gets real path of a directory
func GetRealPath(dir string) string {
	r, e := filepath.Abs(dir)
	if e != nil {
		logger.Debug("GetRealPath failed:  %s", e)
		return ""
	}
	return r
}

// GetObjectAttributes gets attributes of a file
func GetObjectAttributes(path string) *FileAttrs {
	if !common.IsPathExist(path) {
		return nil
	}
	_, name := common.ParseFile(path)
	res := &FileAttrs{
		FullPath:     path,
		RelativePath: path,
		Name:         name,
		Size:         common.GetFileSize(path),
		CRC32C:       common.GetFileCRC32C(path),
		ModTime:      common.GetFileModificationTime(path),
	}
	return res
}

// GetObjectsAttributes gets attributes of all the files under a dir
func GetObjectsAttributes(dir string, isRec bool) []*FileAttrs {
	res := []*FileAttrs{}
	dir = GetRealPath(dir)
	objs := ListObjects(dir, isRec)
	for _, obj := range objs {
		_, name := common.ParseFile(obj)
		res = append(res, &FileAttrs{
			FullPath:     obj,
			RelativePath: common.GetRelativePath(dir, obj),
			Name:         name,
			Size:         common.GetFileSize(obj),
			CRC32C:       0, // low performance so only set default value, populate when necessary
			ModTime:      common.GetFileModificationTime(obj),
		})
	}
	return res
}

// ListObjects lists objects under a prefix
func ListObjects(dir string, isRec bool) []string {
	dir = GetRealPath(dir)
	var stdout []byte
	var err error
	if isRec {
		stdout, err = exec.Command("find", dir, "-type", "f").Output()
	} else {
		stdout, err = exec.Command("find", dir, "-type", "f", "-maxdepth", "1").Output()
	}
	if err != nil {
		logger.Debug("failed with %s", err)
		return []string{}
	}
	res := strings.Split(string(stdout), "\n")
	objs := []string{}
	for _, v := range res {
		v = strings.Trim(v, " \t\n")
		if len(v) > 0 && !common.IsTempFile(v) {
			objs = append(objs, v)
		}
	}
	return objs
}

// GetDiskUsageObjects gets disk usage of objects under a prefix
func GetDiskUsageObjects(dir string) []string {
	dir = GetRealPath(dir)
	stdout, err := exec.Command("du", "-ah", dir).Output()
	if err != nil {
		logger.Debug("failed with %s", err)
		return []string{}
	}
	res := strings.Split(string(stdout), "\n")
	objs := []string{}
	for _, v := range res {
		v = strings.Trim(v, " \t\n")
		if len(v) > 0 {
			objs = append(objs, v)
		}
	}
	return objs
}

// DeleteObject deletes an object
func DeleteObject(path string) {
	_, err := exec.Command("rm", "-rf", path).Output()
	if err != nil {
		logger.Debug("failed with %s", err)
	}
	logger.Info("Removing path[%s]", path)
}

// CopyObject copies an object
func CopyObject(srcPath, dstPath string) {
	folder, _ := common.ParseFile(dstPath)
	if !common.IsPathExist(folder) {
		common.CreateFolder(folder)
	}
	_, err := exec.Command("cp", "-rf", srcPath, dstPath).Output()
	if err != nil {
		logger.Debug("failed with %s", err)
	}
	logger.Info("Copying from path[%s] to path[%s]", srcPath, dstPath)
}

// MoveObject moves an object
func MoveObject(srcPath, dstPath string) {
	_, err := exec.Command("mv", srcPath, dstPath).Output()
	if err != nil {
		logger.Debug("failed with %s", err)
	}
	logger.Info("Moving from path[%s] to path[%s]", srcPath, dstPath)
}

// OutputObject outputs an object
func OutputObject(path string) []byte {
	bs, err := exec.Command("cat", path).Output()
	if err != nil {
		logger.Info("failed with %s", err)
		return nil
	}
	return bs
}

// IsDirectoryOrObject checks if is a directory or an object
func IsDirectoryOrObject(path string) bool {
	return common.IsPathDirectory(path) || common.IsPathFile(path)
}

// IsObject checks if is an object
func IsObject(path string) bool {
	return common.IsPathFile(path)
}

// IsDirectory checks if is a directory
func IsDirectory(path string) bool {
	return common.IsPathDirectory(path)
}
