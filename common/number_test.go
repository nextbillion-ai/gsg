package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToByteSize(t *testing.T) {
	assert.Equal(t, "1024", ToByteSize("1024"))
	assert.Equal(t, "1024", ToByteSize("1k"))
	assert.Equal(t, "1024", ToByteSize("1K"))
	assert.Equal(t, "1024", ToByteSize("1.0K"))
	assert.Equal(t, "1048576", ToByteSize("1.0M"))
	assert.Equal(t, "1153433", ToByteSize("1.1M"))
	assert.Equal(t, "1073741824", ToByteSize("1.0G"))
	assert.Equal(t, "1181116006", ToByteSize("1.1G"))
	assert.Equal(t, "1099511627776", ToByteSize("1.0T"))
	assert.Equal(t, "1209462790553", ToByteSize("1.1T"))
}

func TestFromByteSize(t *testing.T) {
	assert.Equal(t, "0B", FromByteSize("0"))
	assert.Equal(t, "0B", FromByteSize("0.0"))
	assert.Equal(t, "512B", FromByteSize("512"))
	assert.Equal(t, "512B", FromByteSize("512.0"))
	assert.Equal(t, "1K", FromByteSize("1024"))
	assert.Equal(t, "1K", FromByteSize("1024.0"))
	assert.Equal(t, "1K", FromByteSize("1025"))
	assert.Equal(t, "1.5K", FromByteSize("1536"))
}
