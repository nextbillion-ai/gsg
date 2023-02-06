package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseUrl(t *testing.T) {
	tests := []struct {
		url            string
		expectedScheme string
		expectedBucket string
		expectedPrefix string
	}{
		{
			url:            "gs://test-bucket/a/b/c.d",
			expectedScheme: "gs",
			expectedBucket: "test-bucket",
			expectedPrefix: "a/b/c.d",
		},
		{
			url:            "s3://test-bucket/a/b/c.d",
			expectedScheme: "s3",
			expectedBucket: "test-bucket",
			expectedPrefix: "a/b/c.d",
		},
		{
			url:            "gs://test-bucket",
			expectedScheme: "gs",
			expectedBucket: "test-bucket",
			expectedPrefix: "",
		},
		{
			url:            "gs://test-bucket/",
			expectedScheme: "gs",
			expectedBucket: "test-bucket",
			expectedPrefix: "",
		},
		{
			url:            "http://test-bucket/a/b/c.d",
			expectedScheme: "http",
			expectedBucket: "test-bucket",
			expectedPrefix: "a/b/c.d",
		},
		{
			url:            "test-bucket/a/b/c.d",
			expectedScheme: "",
			expectedBucket: "",
			expectedPrefix: "test-bucket/a/b/c.d",
		},
		{
			url:            "/test-bucket/a/b/c.d",
			expectedScheme: "",
			expectedBucket: "",
			expectedPrefix: "/test-bucket/a/b/c.d",
		},
		{
			url:            "test-bucket",
			expectedScheme: "",
			expectedBucket: "",
			expectedPrefix: "test-bucket",
		},
		{
			url:            "/",
			expectedScheme: "",
			expectedBucket: "",
			expectedPrefix: "/",
		},
		{
			url:            "",
			expectedScheme: "",
			expectedBucket: "",
			expectedPrefix: "",
		},
	}
	for _, test := range tests {
		scheme, bucket, prefix := ParseURL(test.url)
		assert.Equal(t, test.expectedScheme, scheme, test.url)
		assert.Equal(t, test.expectedBucket, bucket, test.url)
		assert.Equal(t, test.expectedPrefix, prefix, test.url)
	}
}

func TestParseFile(t *testing.T) {
	tests := []struct {
		file         string
		expectedDir  string
		expectedName string
	}{
		{
			file:         "gs://test-bucket/a/b/c.d",
			expectedDir:  "gs://test-bucket/a/b",
			expectedName: "c.d",
		},
		{
			file:         "test-bucket/a/b/c.d",
			expectedDir:  "test-bucket/a/b",
			expectedName: "c.d",
		},
		{
			file:         "/test-bucket/a/b/c.d",
			expectedDir:  "/test-bucket/a/b",
			expectedName: "c.d",
		},
		{
			file:         "~/test-bucket/a/b/c.d",
			expectedDir:  "~/test-bucket/a/b",
			expectedName: "c.d",
		},
		{
			file:         "test-bucket/a/b/",
			expectedDir:  "test-bucket/a/b",
			expectedName: "b",
		},
		{
			file:         "test-bucket/a/b",
			expectedDir:  "test-bucket/a",
			expectedName: "b",
		},
		{
			file:         "test-bucket/",
			expectedDir:  "test-bucket",
			expectedName: "test-bucket",
		},
		{
			file:         "test-bucket",
			expectedDir:  ".",
			expectedName: "test-bucket",
		},
		{
			file:         "/test-bucket",
			expectedDir:  "/",
			expectedName: "test-bucket",
		},
		{
			file:         "/",
			expectedDir:  "/",
			expectedName: "/",
		},
		{
			file:         ".",
			expectedDir:  ".",
			expectedName: ".",
		},
		{
			file:         "",
			expectedDir:  ".",
			expectedName: ".",
		},
	}
	for _, test := range tests {
		dir, name := ParseFile(test.file)
		assert.Equal(t, test.expectedDir, dir, test.file)
		assert.Equal(t, test.expectedName, name, test.file)
	}
}

func TestJoinPath(t *testing.T) {
	tests := []struct {
		dir      string
		name     string
		expected string
	}{
		{
			dir:      "gs://test-bucket/a/b",
			name:     "c.d",
			expected: "gs://test-bucket/a/b/c.d",
		},
		{
			dir:      "gs://test-bucket/a/b/",
			name:     "c.d",
			expected: "gs://test-bucket/a/b/c.d",
		},
		{
			dir:      "test-bucket/a/b",
			name:     "c.d",
			expected: "test-bucket/a/b/c.d",
		},
		{
			dir:      "test-bucket/a/b/",
			name:     "c.d",
			expected: "test-bucket/a/b/c.d",
		},
		{
			dir:      "/test-bucket/a/b",
			name:     "c.d",
			expected: "/test-bucket/a/b/c.d",
		},
		{
			dir:      "/test-bucket/a/b/",
			name:     "c.d",
			expected: "/test-bucket/a/b/c.d",
		},
		{
			dir:      "/test-bucket/a/b",
			name:     "",
			expected: "/test-bucket/a/b",
		},
		{
			dir:      "/test-bucket/a/b/",
			name:     "",
			expected: "/test-bucket/a/b",
		},
		{
			dir:      "",
			name:     "a",
			expected: "a",
		},
		{
			dir:      "",
			name:     "",
			expected: "",
		},
	}
	for _, test := range tests {
		res := JoinPath(test.dir, test.name)
		assert.Equal(t, test.expected, res, test.expected)
	}
}

func TestGetRelativePath(t *testing.T) {
	tests := []struct {
		dir      string
		path     string
		expected string
	}{
		{
			dir:      "gs://test-bucket/a/b",
			path:     "gs://test-bucket/a/b/c.d",
			expected: "c.d",
		},
		{
			dir:      "gs://test-bucket/a",
			path:     "gs://test-bucket/a/b/c.d",
			expected: "b/c.d",
		},
		{
			dir:      "gs://test-bucket/a/",
			path:     "gs://test-bucket/a/b/c.d",
			expected: "b/c.d",
		},
		{
			dir:      "/test-bucket/a/b",
			path:     "/test-bucket/a/b/c.d",
			expected: "c.d",
		},
		{
			dir:      "",
			path:     "/test-bucket/a/b",
			expected: "/test-bucket/a/b",
		},
		{
			dir:      "test-bucket/a/b",
			path:     "test-bucket/a/b/c.d",
			expected: "c.d",
		},
		{
			dir:      "test-bucket/a",
			path:     "test-bucket/a/b/c.d",
			expected: "b/c.d",
		},
		{
			dir:      "test-bucket/a/",
			path:     "test-bucket/a/b/c.d",
			expected: "b/c.d",
		},
		{
			dir:      "test-bucket1/a/",
			path:     "test-bucket/a/b/c.d",
			expected: "test-bucket/a/b/c.d",
		},
		{
			dir:      "",
			path:     "test-bucket/a/",
			expected: "test-bucket/a/",
		},
		{
			dir:      "",
			path:     "",
			expected: "",
		},
	}
	for _, test := range tests {
		res := GetRelativePath(test.dir, test.path)
		assert.Equal(t, test.expected, res, test.expected)
	}
}

func TestSetPrefixAsDirectory(t *testing.T) {
	assert.Equal(t, "", SetPrefixAsDirectory(""))
	assert.Equal(t, "/", SetPrefixAsDirectory("/"))
	assert.Equal(t, "a/", SetPrefixAsDirectory("a"))
	assert.Equal(t, "a/", SetPrefixAsDirectory("a/"))
	assert.Equal(t, "/a/", SetPrefixAsDirectory("/a"))
	assert.Equal(t, "a/b/", SetPrefixAsDirectory("a/b"))
	assert.Equal(t, "a/b/", SetPrefixAsDirectory("a/b/"))
	assert.Equal(t, "/a/b/", SetPrefixAsDirectory("/a/b"))
}

func TestGetDstPath(t *testing.T) {
	assert.Equal(t, "ghi/jkl.mno", GetDstPath("", "ghi/jkl.mno", ""))
	assert.Equal(t, "ghi/jkl.mno", GetDstPath("", "ghi/jkl.mno", "."))
	assert.Equal(t, "/ghi/jkl.mno", GetDstPath("", "ghi/jkl.mno", "/"))
	assert.Equal(t, "pqr/stu/ghi/jkl.mno", GetDstPath("", "ghi/jkl.mno", "pqr/stu"))
	assert.Equal(t, "pqr/stu/ghi/jkl.mno", GetDstPath("/", "/ghi/jkl.mno", "pqr/stu/"))
	assert.Equal(t, "pqr/stu/ghi/jkl.mno", GetDstPath("abc/def", "abc/def/ghi/jkl.mno", "pqr/stu"))
	assert.Equal(t, "pqr/stu/ghi/jkl.mno", GetDstPath("abc/def/", "abc/def/ghi/jkl.mno", "pqr/stu/"))
	assert.Equal(t, "/pqr/stu/ghi/jkl.mno", GetDstPath("gs://abc/def", "gs://abc/def/ghi/jkl.mno", "/pqr/stu"))
	assert.Equal(t, "/pqr/stu/ghi/jkl.mno", GetDstPath("gs://abc/def/", "gs://abc/def/ghi/jkl.mno", "/pqr/stu/"))
}

func TestIsSubPath(t *testing.T) {
	tests := []struct {
		subPath  string
		path     string
		expected bool
	}{
		{
			subPath:  "gs://abc/def",
			path:     "gs://abc/def",
			expected: true,
		},
		{
			subPath:  "gs://abc/def",
			path:     "gs://abc/de",
			expected: false,
		},
		{
			subPath:  "gs://abc/def",
			path:     "gs://abc/def/",
			expected: false,
		},
		{
			subPath:  "gs://abc/def/",
			path:     "gs://abc/def/",
			expected: true,
		},
		{
			subPath:  "gs://abc/def/ghi",
			path:     "gs://abc/def",
			expected: true,
		},
		{
			subPath:  "gs://abc/def/ghi",
			path:     "gs://abc/de",
			expected: false,
		},
		{
			subPath:  "gs://abc/def/ghi",
			path:     "gs://abc/def/",
			expected: true,
		},
	}
	for i, test := range tests {
		assert.Equal(t, test.expected, IsSubPath(test.subPath, test.path), fmt.Sprintf("test case %d", i))
	}
}
