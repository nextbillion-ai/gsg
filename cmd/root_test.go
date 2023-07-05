package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultiThread(t *testing.T) {
	assert.False(t, enableMultiThread)
	assert.Equal(t, 64, multiThread)
}
