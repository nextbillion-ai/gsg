package common

import (
	"io"
	"math"
	"os"
	"time"
)

// A seam for the tests, as on the write side: the advice is a no-op on every
// platform but linux, so a test has no other way to see that a read was paced
// at all.
var gentleAdviseDropRead = FadviseReadDontNeed

// Gentle says what a gentle read should do. They are separate because an
// upload does not want both everywhere.
//
// Pause leaves the disk to whatever else is using it. Drop leaves the page
// cache to whatever else is using it, and belongs on the *last* read of a
// range -- dropping earlier would send a later read back to the disk for bytes
// that were just there.
//
// An upload reads each range twice: once to checksum it, because the checksum
// is a request header and has to be known before the body is sent, and once to
// send it. So the send drops and the checksum pass does not, while both pause.
//
// Pausing both is deliberate, and was not the first answer here. The first
// paced only the checksum read, on the reasoning that the send reads the pages
// that read had just filled and so costs the disk nothing. That holds only
// while the range stays resident: eight 128 MiB parts in flight is a gigabyte
// competing for whatever cache the machine has, and the memory pressure this
// flag exists to be considerate of is exactly when it will not. When the send
// does go to the disk, the pacing has to be there.
//
// The unit is therefore a byte *read*, not a byte uploaded. A cached send
// pauses for reads that cost the disk nothing, which is the price of not
// having to know which ones those are -- and gentle mode is a request to go
// slower, so erring that way is the safe direction. Measured, it costs
// nothing anyway: 2 GiB to oci ran 34.7s and 31.5s gentle against 35.5s and
// 35.6s plain, because the pauses are spread across the parts in flight and
// the others keep the socket busy while one sleeps.
type Gentle struct {
	Pause bool
	Drop  bool
}

// On reports whether anything is being asked for.
func (g Gentle) On() bool { return g.Pause || g.Drop }

// GentleSection reads a range of a file and asks the kernel to drop the pages
// behind it as it goes.
//
// It is the read-side counterpart of GentleWrite, and a simpler one: the pages
// it is dropping are clean, so a single request frees them, where the write
// side needs a second request over the same range because a drop on dirty
// pages only starts their writeback. There is no closing request here for that
// reason.
//
// It also reports progress, which is not a convenience. The body of an upload
// has to stay rewindable or the SDK turns its own retry off -- it reflects
// into the reader looking for an io.Seeker, and an io.TeeReader is not one, so
// wrapping the file to count bytes is what used to disable retries on exactly
// the path the CLI takes. This counts bytes and still seeks.
type GentleSection struct {
	f        *os.File
	sr       *io.SectionReader
	off      int64
	progress io.Writer
	gentle   Gentle

	// pos is how far into the section the reader has got, and marked is how
	// far the pausing and dropping have reached. Both are reset by a Seek,
	// because a rewind means the whole thing is about to be read again.
	pos    int64
	marked int64
}

// NewGentleSection returns a reader over length bytes of f from off.
//
// A negative length means "to the end of the file", which is not the same as
// passing the size a stat reported: a body capped at a stat cannot notice that
// the file grew, and would upload the original prefix with a checksum that
// matches it -- silently, since everything about that object is consistent.
// Read to the end and a file that grew delivers more than the ContentLength
// promised, which fails the request.
//
// With a zero Gentle it is an ordinary section reader that counts bytes: no
// advice, no pauses. That is deliberate -- it means the upload path has one
// body type rather than two, and the seekability that the SDK's retry depends
// on does not vary with a flag.
//
// progress may be nil.
func NewGentleSection(f *os.File, off, length int64, gentle Gentle, progress io.Writer) *GentleSection {
	if length < 0 {
		// io.SectionReader wants a length; this is how it is told "whatever is
		// there". The tail then arrives as io.EOF rather than as a known end,
		// which is the one case the release below cannot anticipate.
		length = math.MaxInt64 - off
	}
	return &GentleSection{
		f:        f,
		sr:       io.NewSectionReader(f, off, length),
		off:      off,
		progress: progress,
		gentle:   gentle,
	}
}

// Size is the length of the section, as io.SectionReader reports it.
func (g *GentleSection) Size() int64 { return g.sr.Size() }

func (g *GentleSection) Read(p []byte) (int, error) {
	n, err := g.sr.Read(p)
	if n > 0 {
		g.pos += int64(n)
		if g.progress != nil {
			// A progress bar that cannot be written to is not a reason to
			// fail an upload.
			_, _ = g.progress.Write(p[:n])
		}
		if g.gentle.On() && g.pos-g.marked >= GentleWindow {
			g.release()
		}
		// The tail, at the moment the section is used up, rather than when
		// something gets round to reading past it.
		//
		// Measured, net/http does read again and does see the io.EOF below:
		// it copies ContentLength bytes through an io.LimitReader and then
		// reads once more to check for extra ones. So this is not fixing a
		// bug that was there -- it removes the dependency. The body is handed
		// to the SDK wrapped, the wrapper decides how it is read, and nothing
		// about "the last window is dropped" should rest on a consumer making
		// a read it does not need. A body smaller than one window is the case
		// that would otherwise have been paced and dropped not at all.
		if g.gentle.On() && g.pos == g.sr.Size() {
			g.release()
		}
	}
	if err == io.EOF && g.gentle.On() {
		g.release()
	}
	return n, err
}

// release drops what has been read since the last time, and stands aside for
// as long as those bytes are worth.
//
// The pause is proportional rather than fixed so that the rate does not depend
// on how much the caller happens to read per call: gentlePause per
// GentleWindow, the same rate GentleWrite uses.
func (g *GentleSection) release() {
	window := g.pos - g.marked
	if window <= 0 {
		return
	}
	if g.gentle.Drop {
		gentleAdviseDropRead(g.f, g.off+g.marked, window)
	}
	if g.gentle.Pause {
		gentleSleep(time.Duration(int64(gentlePause) * window / GentleWindow))
	}
	g.marked = g.pos
}

// Seek rewinds or moves the reader, and forgets what it had dropped.
//
// The SDK seeks back to the start to retry a part or a body. The pages that
// retry re-reads have been dropped on purpose, so it pays for them again --
// which is the right way round: a retry is rare, and the alternative is
// keeping the whole transfer in the page cache in case one happens.
//
// Seeking backwards takes the progress report back with it, or a retried body
// counts its bytes twice and the bar runs ahead of the upload.
func (g *GentleSection) Seek(offset int64, whence int) (int64, error) {
	at, err := g.sr.Seek(offset, whence)
	if err != nil {
		return at, err
	}
	if at < g.pos && g.gentle.On() {
		// A rewind is a retry, and the pages it is about to re-read were
		// dropped on purpose by the pass that just failed, so this one
		// certainly goes to the disk. Both upload paths already pause, so this
		// changes nothing for them; it is here for a caller that asked only to
		// drop, so that such a reader cannot re-read a file from disk at full
		// speed after having evicted it.
		g.gentle.Pause = true
	}
	if g.progress != nil && at < g.pos {
		if pw, ok := g.progress.(interface{ IncrBy(int64) }); ok {
			pw.IncrBy(at - g.pos)
		}
	}
	g.pos, g.marked = at, at
	return at, nil
}

// FadviseSequentialRead tells the kernel a whole file is about to be read
// straight through, when the caller asked for gentle I/O.
func FadviseSequentialRead(f *os.File, gentle Gentle) {
	if gentle.On() {
		FadviseReadSequential(f)
	}
}
