package common

import (
	"gsutil-go/logger"
	"testing"
	"time"
)

func TestElapsed(t *testing.T) {
	defer Elapsed("test", time.Now())

	logger.Debugging = true
	defer Elapsed("test", time.Now())
}
