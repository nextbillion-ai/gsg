package logger

import (
	"fmt"
	"time"
)

// Debugging switches debugging mode
var Debugging = false

// Output directly output content to stdout
func Output(s string) {
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
	if len(module) != 0 {
		fmt.Printf("[%s] %s: %s\n", time.Now().Format("2006-01-02 15:04:05"), module, s)
	} else {
		fmt.Printf("[%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), s)
	}
}

// Warn warn level log
func Warn(module, s string, vs ...any) {
	s = fmt.Sprintf(s, vs...)
	fmt.Printf("[%s] [WARN] %s: %s\n", time.Now().Format("2006-01-02 15:04:05"), module, s)
}

// Error error level log
func Error(module, s string, vs ...any) {
	s = fmt.Sprintf(s, vs...)
	fmt.Printf("[%s] [ERROR] %s: %s\n", time.Now().Format("2006-01-02 15:04:05"), module, s)
}
