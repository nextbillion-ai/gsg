package common

import (
	"errors"
	"io/fs"
	"net/http"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

// RetryConfig holds configuration for retry behavior
type RetryConfig struct {
	MaxAttempts int           // Maximum number of attempts (including first attempt)
	Delay       time.Duration // Delay between retries
	Backoff     float64       // Multiplier for delay on each retry (1.0 = no backoff)
	// Retryable, when set, ends the retries at the first error it rejects.
	Retryable func(error) bool
}

// DefaultRetryConfig returns a reasonable default retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: 3,
		Delay:       time.Millisecond * 100,
		Backoff:     1.0,
	}
}

// DoWithRetry executes a function with retry logic
// If all retries fail, it returns the last error encountered
func DoWithRetry(operation func() error, config RetryConfig) error {
	if operation == nil {
		return errors.New("operation cannot be nil")
	}

	var lastErr error

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		if err := operation(); err == nil {
			return nil // Success
		} else {
			lastErr = err
			if config.Retryable != nil && !config.Retryable(err) {
				return err
			}

			// Don't sleep after the last attempt
			if attempt < config.MaxAttempts {
				delay := time.Duration(float64(config.Delay) * float64(attempt-1) * config.Backoff)
				time.Sleep(delay)
			}
		}
	}

	return lastErr
}

// DoWithRetrySimple is a simplified version using default retry configuration
func DoWithRetrySimple(operation func() error) error {
	return DoWithRetry(operation, DefaultRetryConfig())
}

// DoTransferWithRetry runs one cp or rsync transfer under the default retry
// schedule, but stops at an error that a second attempt cannot change: the
// caller exits on it either way, and every attempt prints it again.
func DoTransferWithRetry(operation func() error) error {
	config := DefaultRetryConfig()
	config.Retryable = Retryable
	return DoWithRetry(operation, config)
}

// permanentErrors are local conditions no retry within a command can clear.
// fs.ErrNotExist is a missing local path; a missing object comes back from the
// storage libraries as one of the types below instead.
var permanentErrors = []error{
	fs.ErrPermission,
	fs.ErrNotExist,
	fs.ErrInvalid,
	syscall.ENOTDIR,
	syscall.EISDIR,
	syscall.EROFS,
	syscall.EINVAL,
	storage.ErrObjectNotExist,
	storage.ErrBucketNotExist,
}

// permanentStatus are answers the service gives the same way every time for
// the same request. 408, 429 and 5xx are not among them.
var permanentStatus = map[int]bool{
	http.StatusBadRequest:         true,
	http.StatusForbidden:          true,
	http.StatusNotFound:           true,
	http.StatusPreconditionFailed: true,
}

// Retryable reports whether err may go differently on another attempt. Only
// typed errors are judged: anything it cannot recognise, including a network
// error, a timeout or a reset, stays retryable.
func Retryable(err error) bool {
	if err == nil {
		return false
	}
	for _, p := range permanentErrors {
		if errors.Is(err, p) {
			return false
		}
	}
	var ge *googleapi.Error
	if errors.As(err, &ge) {
		return !permanentStatus[ge.Code]
	}
	// the s3 SDK's response error
	var s3e interface{ HTTPStatusCode() int }
	if errors.As(err, &s3e) {
		return !permanentStatus[s3e.HTTPStatusCode()]
	}
	// the oci SDK's service error
	var ocie interface{ GetHTTPStatusCode() int }
	if errors.As(err, &ocie) {
		return !permanentStatus[ocie.GetHTTPStatusCode()]
	}
	return true
}
