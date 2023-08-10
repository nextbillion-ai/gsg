package common

import (
	"gsg/logger"
	"strconv"
	"strings"

	"code.cloudfoundry.org/bytefmt"
)

// ToByteSize converts size string into bytes, e.g., 1K into 1024
func ToByteSize(s string) string {
	_, err := strconv.ParseFloat(s, 64)
	if err == nil {
		return s
	}
	s = strings.ToUpper(s)
	b, err := bytefmt.ToBytes(s)
	if err != nil {
		logger.Debug("failed with %s", err)
		return s
	}
	return strconv.FormatUint(b, 10)
}

// FromByteSize converts size string in bytes into uinit, e.g., 1024 into 1K
func FromByteSize(s string) string {
	b, err := strconv.ParseFloat(s, 64)
	if err != nil {
		logger.Debug("failed with %s", err)
		return s
	}
	return bytefmt.ByteSize(uint64(b))
}
