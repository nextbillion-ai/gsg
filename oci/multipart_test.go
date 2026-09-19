package oci

import (
	"bytes"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nextbillion-ai/gsg/common"

	ocicommon "github.com/oracle/oci-go-sdk/v65/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The whole-object checksum an upload sends is no longer read from the file:
// it is folded from the sums the parts took as they were read. So the fold,
// the part order and the part lengths together have to reproduce the checksum
// of the file -- and the ground truth here is the whole file hashed in one go,
// not a second copy of the same arithmetic.
//
// Getting this wrong is quiet in the worst way: every part validates on
// arrival, the service assembles the object, and only the whole-object
// checksum disagrees -- after the commit has published it.
func TestThePartSumsFoldToTheChecksumOfTheWholeFile(t *testing.T) {
	for _, size := range []int64{
		0, 1, ociMinPartSize - 1, ociMinPartSize, ociMinPartSize + 1,
		3*ociMinPartSize + 12345,
	} {
		content := make([]byte, size)
		for i := range content {
			content[i] = byte(i*31 + i/251)
		}
		path := filepath.Join(t.TempDir(), "part.bin")
		require.NoError(t, os.WriteFile(path, content, 0o644))
		f, err := os.Open(path)
		require.NoError(t, err)

		// Exactly the geometry Upload hands to uploadMultipart.
		partSize, parts := common.PartGeometry(size, -1, ociMinPartSize, ociMaxPartSize)
		sums := make([]uint32, parts)
		lens := make([]int64, parts)
		for i := int64(0); i < parts; i++ {
			off := i * partSize
			length := partSize
			if off+length > size {
				length = size - off
			}
			h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
			_, cerr := io.Copy(h, io.NewSectionReader(f, off, length))
			require.NoError(t, cerr)
			sums[i], lens[i] = h.Sum32(), length
		}
		require.NoError(t, f.Close())

		folded, total := common.FoldCRC32C(sums, lens)
		assert.Equal(t, size, total, "size %d: the parts have to cover the file", size)
		assert.Equal(t, crc32.Checksum(content, common.Castagnoli), folded,
			"size %d in %d part(s) of %d", size, parts, partSize)
	}
}

// Folding removes the second read of the file, and with it the only thing that
// noticed a source rewritten part-way through -- every part would still
// validate on arrival, and the object would be published holding a mix of two
// files. This is the cheap replacement, so it has to actually fire.
func TestSourceMovedSeesASourceRewrittenUnderTheParts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "src.bin")
	require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte("a"), 1024), 0o644))
	before, err := os.Stat(path)
	require.NoError(t, err)

	same, err := os.Stat(path)
	require.NoError(t, err)
	assert.False(t, sourceMoved(before, same), "an untouched file must not fail its own upload")

	// Truncated, appended to, or rewritten in place: the first two change the
	// size, the last only the modification time.
	require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte("a"), 2048), 0o644))
	grown, err := os.Stat(path)
	require.NoError(t, err)
	assert.True(t, sourceMoved(before, grown), "a file that changed size")

	rewritten := filepath.Join(dir, "same-size.bin")
	require.NoError(t, os.WriteFile(rewritten, bytes.Repeat([]byte("a"), 1024), 0o644))
	first, err := os.Stat(rewritten)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(rewritten, bytes.Repeat([]byte("b"), 1024), 0o644))
	require.NoError(t, os.Chtimes(rewritten, first.ModTime().Add(time.Second), first.ModTime().Add(time.Second)))
	second, err := os.Stat(rewritten)
	require.NoError(t, err)
	assert.True(t, sourceMoved(first, second), "same size, different content and mtime")

	// And the gap, stated rather than implied: put the modification time back
	// and the same bytes are indistinguishable from different ones.
	require.NoError(t, os.Chtimes(rewritten, first.ModTime(), first.ModTime()))
	restored, err := os.Stat(rewritten)
	require.NoError(t, err)
	assert.False(t, sourceMoved(first, restored),
		"a rewrite preserving size and mtime is invisible to this check -- if that ever stops being true, the comment on sourceMoved is wrong")
}

// The SDK turns its own retry off for a body it cannot rewind, and it decides
// that by unwrapping io.NopCloser and looking for an io.Seeker. An
// io.TeeReader -- which is how a progress bar used to be attached, on the path
// the CLI always takes -- is not one, so every upload the CLI made was
// uploading without the SDK's retry.
//
// Asked through the SDK's own check rather than asserted about it, and the
// shape it replaced is asked the same question so the test says what it fixed.
func TestTheUploadBodyStaysSeekableForTheSdksRetry(t *testing.T) {
	content := bytes.Repeat([]byte("payload"), 1024)
	path := filepath.Join(t.TempDir(), "body.bin")
	require.NoError(t, os.WriteFile(path, content, 0o644))
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	for _, gentle := range []bool{false, true} {
		body := common.NewGentleSection(f, 0, int64(len(content)), gentle, nil)
		rsc := ocicommon.NewOCIReadSeekCloser(io.NopCloser(body))
		assert.True(t, rsc.Seekable(), "gentle=%v: the SDK must be able to rewind the body to retry it", gentle)
	}

	tee := ocicommon.NewOCIReadSeekCloser(io.NopCloser(io.TeeReader(f, io.Discard)))
	assert.False(t, tee.Seekable(),
		"if a TeeReader ever becomes seekable this test has stopped saying anything")
}
