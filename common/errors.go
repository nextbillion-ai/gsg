package common

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrNoSuchObject is the cause carried by a lookup that found no object at a
// name. The backends answer that as (nil, nil), with no error of their own to
// pass on.
var ErrNoSuchObject = errors.New("no such object")

// ErrNotConfigured is the cause carried when a backend lacks a setting it
// needs to start, which no retry within one command can supply.
var ErrNotConfigured = errors.New("not configured")

type causedError struct {
	msg    string
	causes []error
}

func (e *causedError) Error() string   { return e.msg }
func (e *causedError) Unwrap() []error { return e.causes }

// Caused returns an error that reads as msg and unwraps to its causes, so a
// message written for people keeps the errors Retryable and errors.Is judge.
func Caused(msg string, causes ...error) error {
	kept := make([]error, 0, len(causes))
	for _, c := range causes {
		if c != nil {
			kept = append(kept, c)
		}
	}
	if len(kept) == 0 {
		return errors.New(msg)
	}
	return &causedError{msg: msg, causes: kept}
}

// PanicError describes a recovered panic, keeping its value when it is an error.
func PanicError(what string, recovered any) error {
	if e, ok := recovered.(error); ok {
		return fmt.Errorf("%s: %w", what, e)
	}
	return fmt.Errorf("%s: %v", what, recovered)
}

// FirstFailure collects the outcome of a transfer's concurrent chunks: the
// first error is kept, and its context is cancelled so the other chunks stop.
type FirstFailure struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	err    error
}

// NewFirstFailure returns a FirstFailure whose context is live until a chunk
// fails or Close is called.
func NewFirstFailure() *FirstFailure {
	ctx, cancel := context.WithCancel(context.Background())
	return &FirstFailure{ctx: ctx, cancel: cancel}
}

// Context is the context every chunk's requests run under.
func (f *FirstFailure) Context() context.Context { return f.ctx }

// Stopped reports whether a chunk has already failed. What a chunk sees after
// that is the cancellation, not a failure of its own.
func (f *FirstFailure) Stopped() bool { return f.ctx.Err() != nil }

// Fail records err if it is the first failure, cancels the other chunks, and
// reports whether err was the first.
func (f *FirstFailure) Fail(err error) bool {
	f.mu.Lock()
	first := f.err == nil
	if first {
		f.err = err
	}
	f.mu.Unlock()
	f.cancel()
	return first
}

// Err returns the first failure, or nil.
func (f *FirstFailure) Err() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

// Close releases the context.
func (f *FirstFailure) Close() { f.cancel() }
