package common

import (
	"hash/crc32"
	"io"
	"os"
	"time"
)

// GentleWindow is how much a gentle transfer writes before it sums the window
// and asks the kernel to drop it.
const GentleWindow = 10 * 1024 * 1024

// gentlePause is how long a gentle transfer stands aside at the end of each
// window, so that whatever else is using the disk gets a turn.
const gentlePause = 20 * time.Millisecond

// adviseRange is what a gentle transfer asks the kernel to drop when it closes a
// window: that window and the one before it. The request does not free dirty
// pages, it only starts their writeback, so a window can go no sooner than the
// next request; asked for alone, the file stayed in the page cache whole. Two
// windows keep the requests linear in the chunk, where a range from the start
// of the chunk made every request longer than the last. A window is never
// empty: to fadvise a zero length means up to the end of the file.
func adviseRange(windowStart, windowLen, previousLen int64) (offset, length int64) {
	return windowStart - previousLen, previousLen + windowLen
}

// GentleWrite copies src into dst at offset without leaving the bytes in the
// page cache, and returns the CRC32C of what landed and how much of it there
// was.
//
// Three things happen together, and they have to. The copy is broken into
// windows so the kernel can be asked to drop each one as it is written, which
// is what keeps a large transfer from evicting everything else; a pause at each
// window boundary leaves the disk to whatever else is using it; and each window
// is summed *from the file*, at its offset, while its pages are still cached
// and immediately before they are dropped. That last part is what lets the
// caller settle the transfer without reading the file back afterwards -- a read
// that would come straight off the disk, as large as the file, against whatever
// the pacing was protecting.
//
// dst must already be positioned at offset. verifier reads the same file dst
// writes; a second handle rather than dst itself, because dst is open for
// writing and its offset is in use.
//
// progress may be nil. It is written the bytes as they are copied, so a caller
// that retries a failed chunk has to undo what the failed attempt reported --
// the returned byte count is what was written before the error.
func GentleWrite(dst *os.File, verifier io.ReaderAt, offset int64, src io.Reader, progress io.Writer) (crc uint32, written int64, err error) {
	FadviseWriteSequential(dst)

	buf := make([]byte, 1024*1024)
	sum := crc32.New(Castagnoli)
	var summed, previous int64

	closeWindow := func() error {
		windowStart, windowLen := summed, written-summed
		if windowLen == 0 {
			return nil
		}
		if _, cerr := io.CopyBuffer(sum, io.NewSectionReader(verifier, offset+windowStart, windowLen), buf); cerr != nil {
			return cerr
		}
		summed = written
		at, length := adviseRange(windowStart, windowLen, previous)
		FadviseWriteDontNeed(dst, offset+at, length)
		previous = windowLen
		return nil
	}

	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return 0, written, werr
			}
			if progress != nil {
				// A progress bar that cannot be written to is not a reason to
				// fail a transfer.
				_, _ = progress.Write(buf[:n])
			}
			written += int64(n)
			if written-summed >= GentleWindow {
				if cerr := closeWindow(); cerr != nil {
					return 0, written, cerr
				}
				time.Sleep(gentlePause)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, written, readErr
		}
	}
	// the last, partial window
	if cerr := closeWindow(); cerr != nil {
		return 0, written, cerr
	}
	return sum.Sum32(), written, nil
}

// FoldCRC32C returns the CRC32C of the pieces laid end to end, and their total
// length. The pieces must be in the order they appear in the file; their sums
// may have been taken in any order.
func FoldCRC32C(sums []uint32, lens []int64) (crc uint32, total int64) {
	for i := range sums {
		crc = CombineCRC32C(crc, sums[i], lens[i])
		total += lens[i]
	}
	return crc, total
}
