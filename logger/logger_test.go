package logger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOutput(t *testing.T) {
	Output("test output 1 abc")
}

func TestDebug(t *testing.T) {
	assert.False(t, Debugging)
	Debug("", "test debug %d abc", 1)
	Debugging = true
	Debug("", "test debug %d abc", 1)
}

func TestInfo(t *testing.T) {
	Info("", "test info %d abc", 1)
}

func TestWarn(t *testing.T) {
	Warn("", "test warn %d abc", 1)
}

func TestError(t *testing.T) {
	Error("", "test error %d abc", 1)
}
