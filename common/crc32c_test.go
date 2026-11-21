package common

import (
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCombineCRC32CMatchesASingleSumOverTheWholeInput(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	data := make([]byte, 3<<20+12345)
	rng.Read(data)
	whole := crc32.Checksum(data, Castagnoli)

	for _, cuts := range [][]int{{0}, {1}, {len(data) - 1}, {len(data)}, {1 << 20, 2 << 20}, {5, 6, 7, 1 << 19, 3<<20 + 1}} {
		crc, start := uint32(0), 0
		for _, end := range append(cuts, len(data)) {
			part := data[start:end]
			crc = CombineCRC32C(crc, crc32.Checksum(part, Castagnoli), int64(len(part)))
			start = end
		}
		assert.Equal(t, whole, crc, "cuts %v", cuts)
	}
}

func TestCombineCRC32CIsWhatGCSReportsForAComposedObject(t *testing.T) {
	// the documented example: crc32c("123456789") is 0xe3069283
	assert.Equal(t, uint32(0xe3069283),
		CombineCRC32C(crc32.Checksum([]byte("1234"), Castagnoli), crc32.Checksum([]byte("56789"), Castagnoli), 5))
}

func TestStoreFileCRC32CIsWhatTheNextLookupReturnsWithoutReadingTheFile(t *testing.T) {
	t.Setenv(cacheDirEnv, t.TempDir())
	resetCacheDir()
	t.Cleanup(resetCacheDir)
	path := filepath.Join(t.TempDir(), "input.osrm.geometry")
	assert.NoError(t, os.WriteFile(path, []byte("content whose real sum is not 42"), 0644))

	StoreFileCRC32C(path, 42)
	assert.Equal(t, uint32(42), GetFileCRC32C(path))
}

func TestChunkCRC32CSumsOnlyItsOwnShare(t *testing.T) {
	data := []byte("0123456789abcdefghij")
	for _, limit := range []int64{0, 1, 7, 19, 20, 25} {
		c := NewChunkCRC32C(limit)
		// written in pieces that straddle the limit
		for start := 0; start < len(data); start += 3 {
			end := min(start+3, len(data))
			n, err := c.Write(data[start:end])
			assert.NoError(t, err)
			assert.Equal(t, end-start, n, "every byte is accepted")
		}
		share := data[:min(limit, int64(len(data)))]
		crc, n := c.Sum()
		assert.Equal(t, crc32.Checksum(share, Castagnoli), crc, "limit %d", limit)
		assert.Equal(t, int64(len(share)), n, "limit %d", limit)
	}
}

func TestChunkSumsFoldToTheWholeFile(t *testing.T) {
	data := make([]byte, 1<<20+333)
	rand.New(rand.NewSource(3)).Read(data)
	const chunk = 1 << 16
	var sums []uint32
	var lens []int64
	for start := 0; start < len(data); start += chunk {
		end := min(start+chunk, len(data))
		c := NewChunkCRC32C(int64(end - start))
		// an s3 chunk fetches one byte past its share
		_, _ = c.Write(data[start:min(end+1, len(data))])
		s, n := c.Sum()
		sums, lens = append(sums, s), append(lens, n)
	}
	crc, total := FoldCRC32C(sums, lens)
	assert.Equal(t, crc32.Checksum(data, Castagnoli), crc)
	assert.Equal(t, int64(len(data)), total)
}
