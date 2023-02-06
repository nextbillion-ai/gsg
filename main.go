package main

import (
	"gsutil-go/cmd"
)

func main() {
	// uncomment following line for enabling pprof
	// - go tool pprof -alloc_space/-inuse_space -cum -svg http://localhost:8080/debug/pprof/heap > heap_inuse.svg
	// go http.ListenAndServe("localhost:8080", nil)

	_ = cmd.Execute()
}
