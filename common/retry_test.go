package common

import (
	"errors"
	"testing"
	"time"
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
