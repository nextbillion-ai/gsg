package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsCatCommand(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{"plain", []string{"gsg", "cat", "gs://b/o"}, true},
		{"flags first", []string{"gsg", "--debug", "-m", "cat", "gs://b/o"}, true},
		{"flag values skipped", []string{"gsg", "--chunk-size", "0", "-c", "8", "cat", "gs://b/o"}, true},
		{"flag after", []string{"gsg", "cat", "--window", "4", "gs://b/o"}, true},
		{"another command", []string{"gsg", "cp", "gs://b/cat", "cat"}, false},
		{"none", []string{"gsg"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isCatCommand(tt.args))
		})
	}
}
