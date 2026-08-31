package logger

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Debugging switches debugging mode
var Debugging = false

// lastSaid is the most recent line printed at info level or above.
//
// common.ExitWith consults it so that a command which fails says why exactly
// once. Tracking merely *whether* something was printed is not enough: rsync
// announces "Building synchronization state..." before it does any work, so
// any flag set by that would silence the error that follows -- which is the
// bug in TODO item 20, surviving in the most common path. What matters is
// whether the failure itself has already been described.
//
// Guarded because the worker pool logs from many goroutines. If another
// goroutine logs in between, the error is printed again rather than dropped:
// saying it twice is a nuisance, saying nothing is the defect.
var (
	lastMu   sync.Mutex
	lastSaid string
)

func remember(s string) {
	lastMu.Lock()
	lastSaid = s
	lastMu.Unlock()
}

// AlreadySaid reports whether the most recent line printed already carries
// this text, so a caller can avoid repeating it.
func AlreadySaid(text string) bool {
	if text == "" {
		return false
	}
	lastMu.Lock()
	defer lastMu.Unlock()
	return strings.Contains(lastSaid, text)
}

// Output directly output content to stdout
func Output(s string) {
	// Deliberately not remembered: Output is the command's result, not a
	// report of what happened, so it must not stand in for an explanation.
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
	s = fmt.Sprintf(s, vs...)
	remember(s)
	if len(module) != 0 {
		fmt.Printf("[%s] %s: %s\n", time.Now().Format("2006-01-02 15:04:05"), module, s)
	} else {
		fmt.Printf("[%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), s)
	}
}

// Warn warn level log
func Warn(module, s string, vs ...any) {
	s = fmt.Sprintf(s, vs...)
	remember(s)
	fmt.Printf("[%s] [WARN] %s: %s\n", time.Now().Format("2006-01-02 15:04:05"), module, s)
}

// Error error level log
func Error(module, s string, vs ...any) {
	s = fmt.Sprintf(s, vs...)
	remember(s)
	fmt.Printf("[%s] [ERROR] %s: %s\n", time.Now().Format("2006-01-02 15:04:05"), module, s)
}
