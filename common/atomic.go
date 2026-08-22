package common

import (
	"os"
	"path/filepath"
)

// WriteFileAtomic writes data to path so that no reader ever sees it partially
// written.
//
// os.WriteFile creates or truncates the file first and writes afterwards, so
// anything that killed the process in between -- a failing object calling Exit,
// which is os.Exit from a pool goroutine, or a signal -- left a short file
// behind for every later run to decode. Writing to a temp file in the same
// directory and renaming publishes the content in a single step instead.
//
// The rename replaces a symlink at path rather than following it, and cannot
// cross a filesystem boundary because the temp file is a sibling.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp")
	if err != nil {
		return err
	}
	tempName := f.Name()
	defer func() {
		_ = f.Close()
		// A no-op once the rename below succeeded.
		_ = os.Remove(tempName)
	}()

	if _, err = f.Write(data); err != nil {
		return err
	}
	// CreateTemp uses 0600, so the caller's mode is applied explicitly -- and
	// only now that the content is complete, so a temp file leaked by a killed
	// process is never both short and widely readable. A failure here is
	// returned rather than logged: the caller asked for that mode.
	if err = f.Chmod(perm); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tempName, path)
}
