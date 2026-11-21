package common

import "hash/crc32"

// Castagnoli is the table every CRC32C in gsg is computed with.
var Castagnoli = crc32.MakeTable(crc32.Castagnoli)

// castagnoliReversed is the polynomial in the bit order hash/crc32 works in.
const castagnoliReversed = 0x82f63b78

func gf2MatrixTimes(mat *[32]uint32, vec uint32) uint32 {
	var sum uint32
	for i := 0; vec != 0; i, vec = i+1, vec>>1 {
		if vec&1 != 0 {
			sum ^= mat[i]
		}
	}
	return sum
}

func gf2MatrixSquare(square, mat *[32]uint32) {
	for n := 0; n < 32; n++ {
		square[n] = gf2MatrixTimes(mat, mat[n])
	}
}

// CombineCRC32C returns the CRC32C of A followed by B, given the CRC32C of each
// and the length of B. It lets the parts of a file be summed independently and
// in any order. The method is zlib's crc32_combine.
func CombineCRC32C(crcA, crcB uint32, lenB int64) uint32 {
	if lenB <= 0 {
		return crcA
	}
	var even, odd [32]uint32
	// odd: the operator for one zero bit
	odd[0] = castagnoliReversed
	row := uint32(1)
	for n := 1; n < 32; n++ {
		odd[n] = row
		row <<= 1
	}
	gf2MatrixSquare(&even, &odd) // two zero bits
	gf2MatrixSquare(&odd, &even) // four zero bits

	// apply lenB zero bytes to crcA, squaring the operator for each bit of lenB
	for {
		gf2MatrixSquare(&even, &odd)
		if lenB&1 != 0 {
			crcA = gf2MatrixTimes(&even, crcA)
		}
		lenB >>= 1
		if lenB == 0 {
			break
		}
		gf2MatrixSquare(&odd, &even)
		if lenB&1 != 0 {
			crcA = gf2MatrixTimes(&odd, crcA)
		}
		lenB >>= 1
		if lenB == 0 {
			break
		}
	}
	return crcA ^ crcB
}

// ChunkCRC32C sums the bytes written to one chunk of a download, so the file's
// CRC32C is known when the last chunk lands, without reading it back. It sums
// only the first limit bytes and accepts the rest unsummed: an s3 range is
// inclusive, so every chunk there but the last also fetches the first byte of
// the next one, which that chunk sums as its own.
//
// Write never fails, so it can sit in an io.MultiWriter beside the file.
type ChunkCRC32C struct {
	crc   uint32
	limit int64
	n     int64
}

// NewChunkCRC32C sums up to limit bytes.
func NewChunkCRC32C(limit int64) *ChunkCRC32C {
	return &ChunkCRC32C{limit: limit}
}

func (c *ChunkCRC32C) Write(p []byte) (int, error) {
	if rest := c.limit - c.n; rest > 0 {
		q := p
		if int64(len(q)) > rest {
			q = q[:rest]
		}
		c.crc = crc32.Update(c.crc, Castagnoli, q)
		c.n += int64(len(q))
	}
	return len(p), nil
}

// Sum returns the CRC32C of the bytes summed and how many there were.
func (c *ChunkCRC32C) Sum() (crc uint32, n int64) {
	return c.crc, c.n
}
