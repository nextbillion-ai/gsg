package common

import (
	"strconv"
	"strings"

	"github.com/nextbillion-ai/gsg/logger"

	"code.cloudfoundry.org/bytefmt"
)

func ByteSize(bytes uint64) string {
	unit := ""
	value := float64(bytes)

	switch {
	case bytes >= bytefmt.EXABYTE:
		unit = " EiB"
		value /= bytefmt.EXABYTE
	case bytes >= bytefmt.PETABYTE:
		unit = " PiB"
		value /= bytefmt.PETABYTE
	case bytes >= bytefmt.TERABYTE:
		unit = " TiB"
		value /= bytefmt.TERABYTE
	case bytes >= bytefmt.GIGABYTE:
		unit = " GiB"
		value /= bytefmt.GIGABYTE
	case bytes >= bytefmt.MEGABYTE:
		unit = " MiB"
		value /= bytefmt.MEGABYTE
	case bytes >= bytefmt.KILOBYTE:
		unit = " KiB"
		value /= bytefmt.KILOBYTE
	case bytes >= bytefmt.BYTE:
		unit = " B"
	case bytes == 0:
		return "0 B"
	}
	var result string
	if unit != " B" {
		result = strconv.FormatFloat(value, 'f', 2, 64)
	} else {
		result = strconv.FormatInt(int64(bytes), 10)
	}
	result = strings.TrimSuffix(result, ".0")
	return result + unit
}

// FromByteSize converts size string in bytes into uinit, e.g., 1024 into 1K
func FromByteSize(s string) string {
	b, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		logger.Debug(module, "failed with %s", err)
		return s
	}
	return ByteSize(uint64(b))
}
