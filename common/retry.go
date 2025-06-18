package common

import (
	"errors"
	"time"
)

// RetryConfig holds configuration for retry behavior
type RetryConfig struct {
	MaxAttempts int           // Maximum number of attempts (including first attempt)
	Delay       time.Duration // Delay between retries
	Backoff     float64       // Multiplier for delay on each retry (1.0 = no backoff)
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
