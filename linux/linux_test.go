package linux

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileAttrsSame(t *testing.T) {
	fa1 := &FileAttrs{
		RelativePath: "abc/name1",
		Size:         101,
		CRC32C:       1,
	}
	fa2 := &FileAttrs{
		RelativePath: "abc/name1",
		Size:         101,
		CRC32C:       1,
	}
	fa3 := &FileAttrs{
		RelativePath: "abc/name2",
		Size:         101,
		CRC32C:       1,
	}
	fa4 := &FileAttrs{
		RelativePath: "abc/name1",
		Size:         102,
		CRC32C:       1,
	}
	fa5 := &FileAttrs{
		RelativePath: "abc/name1",
		Size:         101,
		CRC32C:       2,
	}
	assert.Equal(t, fa1, fa2)
	assert.NotEqual(t, fa1, fa3)
	assert.NotEqual(t, fa1, fa4)
	assert.NotEqual(t, fa1, fa5)
}
