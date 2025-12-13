//go:build linux
// +build linux

package common

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	// POSIX fadvise constants
	_POSIX_FADV_SEQUENTIAL = 2
	_POSIX_FADV_DONTNEED   = 4
)

// fadvise is a wrapper for posix_fadvise system call
func fadvise(fd int, offset int64, length int64, advice int) error {
	_, _, errno := syscall.Syscall6(
		syscall.SYS_FADVISE64,
		uintptr(fd),
		uintptr(offset),
		uintptr(length),
		uintptr(advice),
		0, 0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

// fadviseSequential hints the kernel for sequential access
func fadviseSequential(file *os.File) {
	fd := int(file.Fd())
	if fd >= 0 {
		_ = fadvise(fd, 0, 0, _POSIX_FADV_SEQUENTIAL)
	}
}

// fadviseDontNeed tells kernel to drop this data from cache
func fadviseDontNeed(file *os.File, offset, length int64) {
	fd := int(file.Fd())
	if fd >= 0 {
		_ = fadvise(fd, offset, length, _POSIX_FADV_DONTNEED)
	}
}

// FadviseWriteSequential hints kernel for sequential write pattern
func FadviseWriteSequential(file *os.File) {
	fd := int(file.Fd())
	if fd >= 0 {
		_ = fadvise(fd, 0, 0, _POSIX_FADV_SEQUENTIAL)
	}
}

// FadviseWriteDontNeed tells kernel to write back and drop cache for written data
func FadviseWriteDontNeed(file *os.File, offset, length int64) {
	fd := int(file.Fd())
	if fd >= 0 {
		_ = fadvise(fd, offset, length, _POSIX_FADV_DONTNEED)
	}
}

var _ = unsafe.Sizeof(0) // for unused import check
