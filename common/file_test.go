package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsPathExist(t *testing.T) {
	assert.True(t, IsPathExist(""))
	assert.True(t, IsPathExist("."))
	assert.True(t, IsPathExist("../cmd"))
	assert.False(t, IsPathExist("../invalid_who_cares"))
	assert.True(t, IsPathExist("./file_test.go"))
	assert.False(t, IsPathExist("./invalid_who_cares.go"))
}

func TestIsPathDirectory(t *testing.T) {
	assert.True(t, IsPathDirectory("."))
	assert.True(t, IsPathDirectory("../cmd"))
	assert.False(t, IsPathDirectory("../invalid_who_cares"))
	assert.False(t, IsPathDirectory("./file_test.go"))
	assert.False(t, IsPathDirectory("./invalid_who_cares.go"))
}

func TestIsPathFile(t *testing.T) {
	assert.False(t, IsPathFile("."))
	assert.False(t, IsPathFile("../cmd"))
	assert.False(t, IsPathFile("../invalid_who_cares"))
	assert.True(t, IsPathFile("./file_test.go"))
	assert.False(t, IsPathFile("./invalid_who_cares.go"))
}

func TestGetFileSize(t *testing.T) {
	assert.Less(t, int64(0), GetFileSize("file.go"))
	assert.Equal(t, int64(0), GetFileSize("invalid_who_cares.go"))
	assert.Equal(t, int64(0), GetFileSize("invalid_who_cares"))
	assert.Equal(t, int64(0), GetFileSize("."))
}

func TestGetFileMD5(t *testing.T) {
	assert.Less(t, 0, len(GetFileMD5("file.go")))
	assert.Equal(t, 0, len(GetFileMD5("invalid_who_cares.go")))
	assert.Equal(t, 0, len(GetFileMD5("invalid_who_cares")))
	assert.Equal(t, 0, len(GetFileMD5(".")))
}
func TestIsTempFile(t *testing.T) {
	assert.False(t, IsTempFile(""))
	assert.False(t, IsTempFile("file.go"))
	assert.True(t, IsTempFile("file.go_.gstmp"))
	assert.True(t, IsTempFile("abc/file.go_.gstmp"))
	assert.True(t, IsTempFile("/abc/file.go_.gstmp"))
	assert.True(t, IsTempFile("gs://abc/file.go_.gstmp"))
}

func TestGetTempFile(t *testing.T) {
	assert.Equal(t, "", GetTempFile(""))
	assert.Equal(t, "file.go_.gstmp", GetTempFile("file.go"))
	assert.Equal(t, "abc/file.go_.gstmp", GetTempFile("abc/file.go"))
	assert.Equal(t, "/abc/file.go_.gstmp", GetTempFile("/abc/file.go"))
	assert.Equal(t, "gs://abc/file.go_.gstmp", GetTempFile("gs://abc/file.go"))
}
