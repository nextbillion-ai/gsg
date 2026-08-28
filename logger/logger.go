package logger

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Debugging switches debugging mode
var Debugging = false

// reported records whether anything has been said to the user at info level or
// above.
//
// common.ExitWith consults it so that a command which fails prints the reason
// exactly once. Without something like this the choice is between staying
// silent when nothing else spoke -- the bug in TODO item 20 -- and printing
// the same failure twice, because the backends log before returning an error
// that the command layer then reports as well.
//
// Atomic because the worker pool logs from many goroutines.
var reported atomic.Bool

// Reported reports whether anything has been printed at info level or above.
func Reported() bool { return reported.Load() }

// Output directly output content to stdout
func Output(s string) {
	reported.Store(true)
	fmt.Print(s)
}

// Debug debug level log
func Debug(module, s string, vs ...any) {
	if !Debugging {
		return
	}
	s = fmt.Sprintf(s, vs...)
	fmt.Printf("[%s] [DEBUG] %s: %s\n", time.Now().Format("2006-01-02 15:04:05"), module, s)
}

// Info info level log
func Info(module, s string, vs ...any) {
	reported.Store(true)
	s = fmt.Sprintf(s, vs...)
	if len(module) != 0 {
		fmt.Printf("[%s] %s: %s\n", time.Now().Format("2006-01-02 15:04:05"), module, s)
	} else {
		fmt.Printf("[%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), s)
	}
}

// Warn warn level log
func Warn(module, s string, vs ...any) {
	reported.Store(true)
	s = fmt.Sprintf(s, vs...)
	fmt.Printf("[%s] [WARN] %s: %s\n", time.Now().Format("2006-01-02 15:04:05"), module, s)
}

// Error error level log
func Error(module, s string, vs ...any) {
	reported.Store(true)
	s = fmt.Sprintf(s, vs...)
	fmt.Printf("[%s] [ERROR] %s: %s\n", time.Now().Format("2006-01-02 15:04:05"), module, s)
}
