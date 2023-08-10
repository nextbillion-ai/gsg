package common

import (
	"testing"
	"time"

	"github.com/nextbillion-ai/gsg/logger"
)

func TestElapsed(t *testing.T) {
	defer Elapsed("test", time.Now())

	logger.Debugging = true
	defer Elapsed("test", time.Now())
}
