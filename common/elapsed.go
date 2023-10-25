package common

import (
	"time"

	"github.com/nextbillion-ai/gsg/logger"
)

// Elapsed gets time elapsed
func Elapsed(s string, start time.Time) {
	elapsed := time.Since(start)
	if len(s) > 0 {
		logger.Debug(module, "%s with elapsed: %s", s, elapsed)
	} else {
		logger.Debug(module, "Elapsed: %s", elapsed)
	}
}
