package logger

import "fmt"

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
	fmt.Printf("[DEBUG] %s: %s\n", module, s)
}

// Info info level log
func Info(module, s string, vs ...any) {
	s = fmt.Sprintf(s, vs...)
	if len(module) != 0 {
		fmt.Printf("%s: %s\n", module, s)
	} else {
		fmt.Printf("%s\n", s)
	}
}

// Warn warn level log
func Warn(module, s string, vs ...any) {
	s = fmt.Sprintf(s, vs...)
	fmt.Printf("[WARN] %s: %s\n", module, s)
}

// Error error level log
func Error(module, s string, vs ...any) {
	s = fmt.Sprintf(s, vs...)
	fmt.Printf("[ERROR] %s: %s\n", module, s)
}
