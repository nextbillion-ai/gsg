package common

import (
	"os"
	"runtime/debug"

	"github.com/nextbillion-ai/gsg/logger"
)

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
	os.Exit(0)
}

// Exit exits the program with non-zero status code
func Exit() {
	os.Exit(1)
}
