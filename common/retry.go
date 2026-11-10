package common

import (
	"errors"
	"io/fs"
	"net/http"
	"slices"
	"strings"
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

// permanentErrors are conditions no retry within a command can clear.
// fs.ErrNotExist is a missing local path; a missing object comes back from the
// storage libraries as one of the types below instead. EINVAL and
// fs.ErrInvalid are not here: they also come from a closed or nil file and from
// an O_DIRECT write that a later attempt can make differently, so only gsg's
// own refusal of its arguments, ErrInvalidRequest, counts.
var permanentErrors = []error{
	fs.ErrPermission,
	fs.ErrNotExist,
	syscall.ENOTDIR,
	syscall.EISDIR,
	syscall.EROFS,
	storage.ErrObjectNotExist,
	storage.ErrBucketNotExist,
	ErrNoSuchObject,
	ErrNotConfigured,
	ErrInvalidRequest,
}

// permanentStatus are answers the service gives the same way every time for
// the same request. 408, 429 and 5xx are not among them, and neither is 412:
// a download reads the generation (gs) or ETag (oci) its lookup saw, an object
// overwritten while it is read answers 412, and another attempt looks the
// object up again and reads the new version.
var permanentStatus = map[int]bool{
	http.StatusBadRequest: true,
	http.StatusForbidden:  true,
	http.StatusNotFound:   true,
}

// gcsRateLimitReasons mark a GCS 403 that is throttling rather than refusal.
var gcsRateLimitReasons = []string{"rateLimitExceeded", "userRateLimitExceeded"}

// s3RetryableCodes are s3 error codes another attempt can get past whatever
// their status: throttling, and a clock skew the SDK corrects on the retry.
var s3RetryableCodes = map[string]bool{
	"SlowDown":                  true,
	"Throttling":                true,
	"ThrottlingException":       true,
	"RequestLimitExceeded":      true,
	"RequestThrottled":          true,
	"RequestTimeTooSkewed":      true,
	"RequestExpired":            true,
	"RequestThrottledException": true,
}

// gcsRateLimited reports whether a GCS error is a rate limit. The JSON API
// parses the reason into Errors; a read over the XML API is not parsed and
// carries it only in the body.
func gcsRateLimited(ge *googleapi.Error) bool {
	for _, item := range ge.Errors {
		if slices.Contains(gcsRateLimitReasons, item.Reason) {
			return true
		}
	}
	for _, reason := range gcsRateLimitReasons {
		if strings.Contains(ge.Body, reason) {
			return true
		}
	}
	return false
}

// Retryable reports whether err may go differently on another attempt. Only
// typed errors are judged: anything it cannot recognise, including a network
// error, a timeout or a reset, stays retryable. oci throttles with 429 and
// 409 IncorrectState, never 403, so its status alone decides.
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
		if gcsRateLimited(ge) {
			return true
		}
		return !permanentStatus[ge.Code]
	}
	// the s3 SDK's API error code
	var coded interface{ ErrorCode() string }
	if errors.As(err, &coded) && s3RetryableCodes[coded.ErrorCode()] {
		return true
	}
	// the s3 SDK's response error
	var s3e interface{ HTTPStatusCode() int }
	if errors.As(err, &s3e) {
		return !permanentStatus[s3e.HTTPStatusCode()]
	}
	// the v1 aws SDK's request failure, which the s3 region lookup returns
	var v1e interface{ StatusCode() int }
	if errors.As(err, &v1e) {
		return !permanentStatus[v1e.StatusCode()]
	}
	// the oci SDK's service error
	var ocie interface{ GetHTTPStatusCode() int }
	if errors.As(err, &ocie) {
		return !permanentStatus[ocie.GetHTTPStatusCode()]
	}
	return true
}
