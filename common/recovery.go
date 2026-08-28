package common

import (
	"os"
	"runtime/debug"

	"github.com/nextbillion-ai/gsg/logger"
)

var AppMode bool

// Recovery recovers from exceptions
func Recovery() {
	if r := recover(); r != nil {
		logger.Debug(module, "stacktrace from panic: \n"+string(debug.Stack()))
		logger.Error(module, "[RECOVERED] with %s", r)
		os.Exit(1)
	}
}

// Finish exits the program with zero status code
func Finish() {
	if AppMode {
		os.Exit(0)
	}
}

// Exit exits the program with non-zero status code
func Exit() {
	if AppMode {
		os.Exit(1)
	}
}

// ExitWith reports err and then exits with a non-zero status code.
//
// Exit on its own prints nothing, so a command that discarded the error it was
// given ended in an empty screen and a 1. That was survivable only by luck:
// the gs and s3 backends log inside the calls their List and Attributes make,
// so an error had usually been reported by the time it reached the command
// layer -- by something else, further down. Where that did not happen the
// failure was invisible. Measured before this: `gsg du <dir with an unreadable
// subdirectory>` and `gsg cp -r` of the same both exited 1 saying nothing at
// all.
//
// It reports only when nothing else has. The backends log before returning an
// error that the command layer then hands here, so logging unconditionally
// would print the same failure twice -- and the point of this is the case
// where nobody said anything at all, not to say it again louder.
//
// A nil error still exits. Some callers have already said what went wrong and
// only want the status code, and refusing to exit for them would be worse than
// a missing line.
func ExitWith(err error) {
	if err != nil && !logger.Reported() {
		logger.Error(module, "%s", err)
	}
	Exit()
}
