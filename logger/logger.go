package logger

import "fmt"

// Debugging switches debugging mode
var Debugging = false

// Output directly output content to stdout
func Output(s string) {
	fmt.Print(s)
}

// Debug debug level log
func Debug(s string, vs ...any) {
	if !Debugging {
		return
	}
	s = fmt.Sprintf(s, vs...)
	fmt.Printf("[DEBUG] %s\n", s)
}

// Info info level log
func Info(s string, vs ...any) {
	s = fmt.Sprintf(s, vs...)
	fmt.Printf("%s\n", s)
}

// Warn warn level log
func Warn(s string, vs ...any) {
	s = fmt.Sprintf(s, vs...)
	fmt.Printf("[WARN] %s\n", s)
}

// Error error level log
func Error(s string, vs ...any) {
	s = fmt.Sprintf(s, vs...)
	fmt.Printf("[ERROR] %s\n", s)
}
