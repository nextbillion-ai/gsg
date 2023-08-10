package common

import (
	"gsg/logger"
	"os"
	"runtime/debug"
)

// Recovery recovers from exceptions
func Recovery() {
	if r := recover(); r != nil {
		logger.Debug("stacktrace from panic: \n" + string(debug.Stack()))
		logger.Error("[RECOVERED] with %s", r)
		os.Exit(1)
	}
}

func Finish() {
	os.Exit(0)
}

// Exit exits the program with non-zero status code
func Exit() {
	os.Exit(1)
}
