package linux

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math/big"
	"os"
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
	ModTime      time.Time
	CalcCRC32C   func() uint32
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
		Size:       attrs.Size,
		ModTime:    attrs.ModTime,
		CalcCRC32C: attrs.CalcCRC32C,
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
		CalcCRC32C:   func() uint32 { return common.GetFileCRC32C(prefix) },
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

func (l *Linux) AttemptLock(bucket, object string, ttl time.Duration) error {
	generation, err := l.DoAttemptLock(bucket, object, ttl)
	if err != nil {
		logger.Info(module, "AttemptLock: failed: %s", err)
		return err
	}

	// Upon successful write, store generation in /tmp (like in the GCS version)
	logger.Debug(module, "AttemptLock: storing generation %d", generation)
	cacheFileName := common.GenTempFileName(bucket, "/", object) // or however you build that path
	generationBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(generationBytes, uint64(generation))

	if err := os.WriteFile(cacheFileName, generationBytes, os.ModePerm); err != nil {
		logger.Info(module, "AttemptLock: cache lock generation failed: %s", err)
		return err
	}
	return nil
}

func (l *Linux) DoAttemptLock(bucket, object string, ttl time.Duration) (int64, error) {
	// If you need to do any one-time init for Linux, do it here
	if err := l.Init(); err != nil {
		return 0, err
	}

	lockFile := object
	if common.IsPathExist(lockFile) {
		// The lock file already exists; check if it's expired
		info, err := os.Stat(lockFile)
		if err != nil {
			return 0, err
		}

		// Compare mod time + ttl to now
		if time.Now().After(info.ModTime().Add(ttl)) {
			// It's expired; remove the old lock
			logger.Debug(module, "DoAttemptLock: lock expired; removing file %s", lockFile)
			if err := os.Remove(lockFile); err != nil {
				return 0, err
			}
		} else {
			// Lock isn't expired; fail to acquire
			return 0, errors.New("DoAttemptLock: lock file exists and has not expired")
		}
	}

	// Create a new lock file, generating a random 64-bit value as "generation"
	generation, err := randomInt64()
	if err != nil {
		return 0, err
	}

	// We’ll store that generation inside the file as binary, similar to how GCS code does
	generationBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(generationBytes, uint64(generation))

	// Write the lock file. If someone else created it between IsPathExist and now,
	// we’d normally handle that race condition. But for a local filesystem scenario,
	// you can decide if you need finer locking or not.
	if err := os.WriteFile(lockFile, generationBytes, 0o644); err != nil {
		return 0, err
	}

	logger.Debug(module, "DoAttemptLock: acquired lock %s with generation %d", lockFile, generation)
	return generation, nil
}

func (l *Linux) DoAttemptUnlock(bucket, object string, generation int64) error {
	// 1. If needed, do any init or checks
	if err := l.Init(); err != nil {
		return err
	}

	lockFile := object

	// 2. Check if lockFile exists
	info, err := os.Stat(lockFile)
	if os.IsNotExist(err) {
		// If the file doesn't exist, you can decide:
		// - Return nil (meaning "nothing to unlock")
		// - Or return an error to indicate mismatch
		logger.Debug(module, "DoAttemptUnlock: lock file does not exist, nothing to unlock.")
		return nil
	}
	if err != nil {
		return err
	}

	logger.Debug(module, "DoAttemptUnlock: found lock file (%s), size=%d", lockFile, info.Size())

	// 3. Read the stored generation from the lock file
	data, err := os.ReadFile(lockFile)
	if err != nil {
		return err
	}
	if len(data) < 8 {
		// If for some reason the file is invalid or truncated
		return errors.New("DoAttemptUnlock: lock file is invalid (less than 8 bytes)")
	}
	fileGen := int64(binary.LittleEndian.Uint64(data))

	logger.Debug(module, "DoAttemptUnlock: lock file generation=%d, expected=%d", fileGen, generation)

	// 4. Compare the generation from the lock file with what the caller expects
	if fileGen != generation {
		// We mimic GCS "GenerationMatch" logic: mismatch => fail
		return errors.New("DoAttemptUnlock: generation mismatch - lock file may have changed")
	}

	// 5. If it matches, remove the lock file (like deleting the GCS object)
	if err := os.Remove(lockFile); err != nil {
		return err
	}

	logger.Debug(module, "DoAttemptUnlock: successfully removed lock file %s", lockFile)
	return nil
}

func (l *Linux) AttemptUnLock(bucket, object string) error {
	// 1. Read the generation from the local /tmp file
	cacheFileName := common.GenTempFileName(bucket, "/", object)
	generationBytes, err := os.ReadFile(cacheFileName)
	if err != nil {
		logger.Debug(module, "AttemptUnLock: no lock cache found at %s (nothing to unlock)", cacheFileName)
		return nil
	}

	if len(generationBytes) < 8 {
		// If for some reason your cache file is corrupted, you might handle it here
		logger.Debug(module, "AttemptUnLock: lock cache file is invalid (less than 8 bytes)")
		return nil
	}

	generation := int64(binary.LittleEndian.Uint64(generationBytes))
	logger.Debug(module, "AttemptUnLock: read cached generation %d from %s", generation, cacheFileName)

	// 2. Call DoAttemptUnlock with that generation
	err = l.DoAttemptUnlock(bucket, object, generation)
	if err != nil {
		logger.Debug(module, "AttemptUnLock: error unlocking: %v", err)
		return err
	}

	// 3. (Optional) Remove the local cache file if the unlock succeeded
	if removeErr := os.Remove(cacheFileName); removeErr != nil {
		logger.Debug(module, "AttemptUnLock: unable to remove lock cache file: %v", removeErr)
		// Not a fatal error; decide if you want to return or just log
	}

	return nil
}

// randomInt64 returns a cryptographically random int64 in the range [0..maxInt64].
func randomInt64() (int64, error) {
	// maxInt64 is 2^63 - 1
	bigVal, err := rand.Int(rand.Reader, big.NewInt(1<<63-1))
	if err != nil {
		return 0, err
	}
	return bigVal.Int64(), nil
}
