package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFromByteSize(t *testing.T) {
	assert.Equal(t, "0 B", FromByteSize("0"))
	assert.Equal(t, "0 B", FromByteSize("0"))
	assert.Equal(t, "512.00 B", FromByteSize("512"))
	assert.Equal(t, "1.00 KiB", FromByteSize("1024"))
	assert.Equal(t, "1.00 KiB", FromByteSize("1025"))
	assert.Equal(t, "1.50 KiB", FromByteSize("1536"))
}
