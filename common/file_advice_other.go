// +build !linux

package common

import "os"

// fadviseSequential is a no-op on non-Linux platforms
func fadviseSequential(file *os.File) {
	// Not supported on this platform
}

// fadviseDontNeed is a no-op on non-Linux platforms
func fadviseDontNeed(file *os.File, offset, length int64) {
	// Not supported on this platform
}

