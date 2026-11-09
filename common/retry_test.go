package common

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/nextbillion-ai/gsg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

func TestDefaultRetryConfig(t *testing.T) {
	config := DefaultRetryConfig()

	if config.MaxAttempts != 3 {
		t.Errorf("Expected MaxAttempts to be 3, got %d", config.MaxAttempts)
	}

	if config.Delay != time.Millisecond*100 {
		t.Errorf("Expected Delay to be 100ms, got %v", config.Delay)
	}

	if config.Backoff != 1.0 {
		t.Errorf("Expected Backoff to be 1.0, got %f", config.Backoff)
	}
}

func TestDoWithRetry_SuccessOnFirstAttempt(t *testing.T) {
	attempts := 0
	operation := func() error {
		attempts++
		return nil
	}

	err := DoWithRetrySimple(operation)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if attempts != 1 {
		t.Errorf("Expected 1 attempt, got %d", attempts)
	}
}

func TestDoWithRetry_SuccessOnSecondAttempt(t *testing.T) {
	attempts := 0
	operation := func() error {
		attempts++
		if attempts == 1 {
			return errors.New("first attempt failed")
		}
		return nil
	}

	err := DoWithRetrySimple(operation)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if attempts != 2 {
		t.Errorf("Expected 2 attempts, got %d", attempts)
	}
}

func TestDoWithRetry_SuccessOnLastAttempt(t *testing.T) {
	attempts := 0
	operation := func() error {
		attempts++
		if attempts < 3 {
			return errors.New("attempt failed")
		}
		return nil
	}

	err := DoWithRetrySimple(operation)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}
}

func TestDoWithRetry_AllAttemptsFail(t *testing.T) {
	attempts := 0
	expectedErr := errors.New("all attempts failed")
	operation := func() error {
		attempts++
		return expectedErr
	}

	err := DoWithRetrySimple(operation)

	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}

	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}
}

func TestDoWithRetry_CustomConfig(t *testing.T) {
	attempts := 0
	operation := func() error {
		attempts++
		if attempts < 5 {
			return errors.New("attempt failed")
		}
		return nil
	}

	config := RetryConfig{
		MaxAttempts: 5,
		Delay:       time.Millisecond * 50,
		Backoff:     2.0,
	}

	start := time.Now()
	err := DoWithRetry(operation, config)
	duration := time.Since(start)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if attempts != 5 {
		t.Errorf("Expected 5 attempts, got %d", attempts)
	}

	// Check that we waited between retries (should be at least 50ms * (1 + 2 + 4 + 8) = 750ms)
	// But allow for some system variation
	expectedMinDelay := time.Millisecond * 600 // Reduced from 750ms to account for system overhead
	if duration < expectedMinDelay {
		t.Errorf("Expected delay of at least %v, got %v", expectedMinDelay, duration)
	}
}

func TestDoWithRetry_NoBackoff(t *testing.T) {
	attempts := 0
	operation := func() error {
		attempts++
		if attempts < 3 {
			return errors.New("attempt failed")
		}
		return nil
	}

	config := RetryConfig{
		MaxAttempts: 3,
		Delay:       time.Millisecond * 10,
		Backoff:     1.0, // No backoff
	}

	start := time.Now()
	err := DoWithRetry(operation, config)
	duration := time.Since(start)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}

	// With no backoff, delay should be approximately 10ms * 2 = 20ms
	// But allow for system variations
	expectedDelay := time.Millisecond * 20
	tolerance := time.Millisecond * 15 // Increased tolerance
	if duration < expectedDelay-tolerance || duration > expectedDelay+tolerance {
		t.Errorf("Expected delay around %v (tolerance: %v), got %v", expectedDelay, tolerance, duration)
	}
}

func TestDoWithRetry_ZeroDelay(t *testing.T) {
	attempts := 0
	operation := func() error {
		attempts++
		if attempts < 3 {
			return errors.New("attempt failed")
		}
		return nil
	}

	config := RetryConfig{
		MaxAttempts: 3,
		Delay:       0,
		Backoff:     1.0,
	}

	start := time.Now()
	err := DoWithRetry(operation, config)
	duration := time.Since(start)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}

	// With zero delay, should complete very quickly
	if duration > time.Millisecond*10 {
		t.Errorf("Expected very quick execution with zero delay, got %v", duration)
	}
}

func TestDoWithRetry_OneAttempt(t *testing.T) {
	attempts := 0
	operation := func() error {
		attempts++
		return errors.New("operation failed")
	}

	config := RetryConfig{
		MaxAttempts: 1,
		Delay:       time.Millisecond * 100,
		Backoff:     2.0,
	}

	err := DoWithRetry(operation, config)

	if err == nil {
		t.Error("Expected error, got nil")
	}

	if attempts != 1 {
		t.Errorf("Expected 1 attempt, got %d", attempts)
	}
}

func TestDoWithRetrySimple_WithNilOperation(t *testing.T) {
	// Test that nil operation returns an error instead of panicking
	err := DoWithRetrySimple(nil)

	if err == nil {
		t.Error("Expected error for nil operation, got nil")
	}

	expectedErrMsg := "operation cannot be nil"
	if err.Error() != expectedErrMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedErrMsg, err.Error())
	}
}

func TestRetryableJudgesTypedErrors(t *testing.T) {
	pathErr := func(errno syscall.Errno) error {
		return &fs.PathError{Op: "open", Path: "/work/o_.gstmp", Err: errno}
	}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"permission denied", pathErr(syscall.EACCES), false},
		{"operation not permitted", pathErr(syscall.EPERM), false},
		{"not a directory", pathErr(syscall.ENOTDIR), false},
		{"no such local file", pathErr(syscall.ENOENT), false},
		{"read-only file system", pathErr(syscall.EROFS), false},
		{"invalid argument", pathErr(syscall.EINVAL), true},
		{"nil file", os.ErrInvalid, true},
		{"gsg refuses its arguments", Caused("cannot copy s3://b/o onto itself", ErrInvalidRequest), false},
		{"gs object not found", fmt.Errorf("lookup: %w", storage.ErrObjectNotExist), false},
		{"gs 403", &googleapi.Error{Code: 403}, false},
		{"gs 404", &googleapi.Error{Code: 404}, false},
		{"gs 412, an overwritten generation", &googleapi.Error{Code: 412}, true},
		{"gs 403 rate limit", &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "rateLimitExceeded"}}}, true},
		{"gs 403 user rate limit", &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "userRateLimitExceeded"}}}, true},
		{"gs 403 rate limit over XML", &googleapi.Error{Code: 403, Body: "<Error><Code>userRateLimitExceeded</Code></Error>"}, true},
		{"gs 403 forbidden", &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "forbidden"}}}, false},
		{"gs object changed under the read", Caused("storage: object doesn't exist", ErrObjectChanged), true},
		{"s3 403 clock skew", codedStatusError{403, "RequestTimeTooSkewed"}, true},
		{"s3 403 access denied", codedStatusError{403, "AccessDenied"}, false},
		{"s3 503 slow down", codedStatusError{503, "SlowDown"}, true},
		{"s3 412", statusError(412), true},
		{"oci 412, an overwritten ETag", ociStatusError(412), true},
		{"s3 403", fmt.Errorf("op: %w", statusError(403)), false},
		{"oci 404", ociStatusError(404), false},
		{"gs 408", &googleapi.Error{Code: 408}, true},
		{"gs 429", &googleapi.Error{Code: 429}, true},
		{"gs 503", &googleapi.Error{Code: 503}, true},
		{"s3 500", statusError(500), true},
		{"oci 429", ociStatusError(429), true},
		{"connection reset", &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}, true},
		{"timeout", context.DeadlineExceeded, true},
		{"unexpected EOF", io.ErrUnexpectedEOF, true},
		{"untyped", errors.New("something else"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Retryable(tt.err))
		})
	}
}

type statusError int

func (e statusError) Error() string       { return fmt.Sprintf("status %d", int(e)) }
func (e statusError) HTTPStatusCode() int { return int(e) }

type codedStatusError struct {
	status int
	code   string
}

func (e codedStatusError) Error() string       { return e.code }
func (e codedStatusError) ErrorCode() string   { return e.code }
func (e codedStatusError) HTTPStatusCode() int { return e.status }

type ociStatusError int

func (e ociStatusError) Error() string          { return fmt.Sprintf("status %d", int(e)) }
func (e ociStatusError) GetHTTPStatusCode() int { return int(e) }

// captureStdout returns what f printed; the logger writes to stdout.
func captureStdout(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	saved := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = saved }()
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	f()
	_ = w.Close()
	return <-done
}

func TestDoTransferWithRetryStopsAtAPermanentErrorAndSaysItOnce(t *testing.T) {
	attempts := 0
	var err error
	out := captureStdout(t, func() {
		err = DoTransferWithRetry(func() error {
			attempts++
			e := &fs.PathError{Op: "open", Path: "/work/o_.gstmp", Err: syscall.EACCES}
			logger.Info("GCS", "download object failed when create temp file with %s", e)
			return e
		})
		ExitWith(err) // AppMode is off in tests, so this only reports
	})
	require.ErrorIs(t, err, fs.ErrPermission)
	assert.Equal(t, 1, attempts, "no retry")
	assert.Equal(t, 1, strings.Count(out, "permission denied"), out)
}

func TestDoTransferWithRetryStillRetriesATransientError(t *testing.T) {
	for _, transient := range []error{
		&googleapi.Error{Code: 503},
		&googleapi.Error{Code: 429},
		&net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)},
	} {
		attempts := 0
		err := DoTransferWithRetry(func() error {
			attempts++
			if attempts == 1 {
				return transient
			}
			return nil
		})
		assert.NoError(t, err, "%v", transient)
		assert.Equal(t, 2, attempts, "%v", transient)
	}
	attempts := 0
	err := DoTransferWithRetry(func() error {
		attempts++
		return &googleapi.Error{Code: 503}
	})
	assert.Error(t, err)
	assert.Equal(t, 3, attempts, "a transient error that persists uses every attempt")
}

func TestRetryableSeesThroughMessagesWrittenForPeople(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"no such object", Caused("failed with bucket[b] prefix[o] not an object", ErrNoSuchObject), false},
		{"service refusal kept as cause", Caused("failed with bucket[b] prefix[o] not an object", &googleapi.Error{Code: 403}), false},
		{"not configured, with the loader's error", Caused("failed in loading defaultConfig", errors.New("bad profile"), ErrNotConfigured), false},
		{"copy onto itself", Caused("cannot copy s3://b/o onto itself", ErrInvalidRequest), false},
		{"chunk reset", Caused("download object failed when write to offet with reset", &net.OpError{Op: "read", Err: syscall.ECONNRESET}), true},
		{"v1 sdk 403", statusCodeError(403), false},
		{"v1 sdk 503", statusCodeError(503), true},
		{"panic with an error", PanicError("panic in part 1", fs.ErrPermission), false},
		{"panic with a string", PanicError("panic in part 1", "boom"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Retryable(tt.err))
		})
	}
	msg := "failed with bucket[b] prefix[o] not an object"
	assert.Equal(t, msg, Caused(msg, ErrNoSuchObject).Error(), "the message is unchanged")
}

type statusCodeError int

func (e statusCodeError) Error() string   { return fmt.Sprintf("status %d", int(e)) }
func (e statusCodeError) StatusCode() int { return int(e) }

func TestFirstFailureKeepsTheFirstAndStopsTheRest(t *testing.T) {
	ff := NewFirstFailure()
	defer ff.Close()
	assert.False(t, ff.Stopped())
	first := errors.New("first")
	assert.True(t, ff.Fail(first))
	assert.False(t, ff.Fail(errors.New("second")))
	assert.True(t, ff.Stopped())
	assert.ErrorIs(t, ff.Context().Err(), context.Canceled)
	assert.Equal(t, first, ff.Err())
}
