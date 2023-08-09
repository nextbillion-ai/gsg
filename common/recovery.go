package common

import (
	"gsutil-go/logger"
	"os"
)

// Recovery recovers from exceptions
func Recovery() {
	if r := recover(); r != nil {
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
