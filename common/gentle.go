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

// gentlePause is how long a gentle transfer stands aside per window written,
// so that whatever else is using the disk gets a turn.
const gentlePause = 20 * time.Millisecond

// Seams for the tests. The advice is a no-op on every platform but linux, and
// a sleep is not observable, so a test has no other way to see that a transfer
// was paced at all -- which is exactly the defect this pacing has had twice
// now: present, and doing nothing.
var (
	gentleSleep      = time.Sleep
	gentleAdviseDrop = FadviseWriteDontNeed
)

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
	var summed, previous, paced int64

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
		gentleAdviseDrop(dst, offset+at, length)
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
				gentleSleep(gentlePause)
				paced = written
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

	// Pace what the loop did not. The sleep in the loop fires only on a full
	// window, so the remainder of any call -- or the whole of one shorter than
	// a window, which is every chunk of a download at a 1 MiB --chunk-size --
	// would otherwise cost nothing at all. Proportional, so the rate is the
	// same whatever the caller's chunk size: gentlePause per GentleWindow.
	if unpaced := written - paced; unpaced > 0 {
		gentleSleep(time.Duration(int64(gentlePause) * unpaced / GentleWindow))
	}

	// Then one more request over everything written. A drop request on dirty
	// pages only starts their writeback, so a window can go no sooner than the
	// next request covering it -- which every window gets from the window
	// after it, except the last, which has none. Without this each call leaves
	// its tail in the page cache, and a call shorter than a window leaves all
	// of it: pacing that is present and does nothing.
	//
	// It comes after the pause on purpose. Two requests in immediate
	// succession can both find the same pages still dirty, and then neither
	// drops anything; the pause is time the writeback already had. Waiting for
	// it outright means sync_file_range, which is linux-only and not what this
	// package does today -- so this is the cheap version of the same idea, and
	// on a busy enough disk the tail can still survive it.
	if written > 0 {
		gentleAdviseDrop(dst, offset, written)
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
