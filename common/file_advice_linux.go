// +build linux

package common

import (
	"os"
	"syscall"
)

// fadviseSequential hints the kernel for sequential access
func fadviseSequential(file *os.File) {
	fd := int(file.Fd())
	if fd >= 0 {
		// POSIX_FADV_SEQUENTIAL: optimize for sequential read
		_ = syscall.Fadvise(fd, 0, 0, syscall.FADV_SEQUENTIAL)
	}
}

// fadviseDontNeed tells kernel to drop this data from cache
func fadviseDontNeed(file *os.File, offset, length int64) {
	fd := int(file.Fd())
	if fd >= 0 {
		// POSIX_FADV_DONTNEED: don't keep in cache
		_ = syscall.Fadvise(fd, offset, length, syscall.FADV_DONTNEED)
	}
}

