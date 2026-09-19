package common

import (
	"io"
	"os"
	"time"
)

// A seam for the tests, as on the write side: the advice is a no-op on every
// platform but linux, so a test has no other way to see that a read was paced
// at all.
var gentleAdviseDropRead = FadviseReadDontNeed

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
	gentle   bool

	// pos is how far into the section the reader has got, and dropped is how
	// far the drop requests have reached. Both are reset by a Seek, because a
	// rewind means the whole thing is about to be read again.
	pos     int64
	dropped int64
	paced   int64
}

// NewGentleSection returns a reader over length bytes of f from off.
//
// When gentle is false it is an ordinary section reader that counts bytes: no
// advice, no pauses. That is deliberate -- it means the upload path has one
// body type rather than two, and the seekability that the SDK's retry depends
// on does not vary with a flag.
//
// progress may be nil.
func NewGentleSection(f *os.File, off, length int64, gentle bool, progress io.Writer) *GentleSection {
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
		if g.gentle && g.pos-g.dropped >= GentleWindow {
			g.release()
		}
	}
	if err == io.EOF && g.gentle {
		// The tail, which is never a whole window.
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
	if window := g.pos - g.dropped; window > 0 {
		gentleAdviseDropRead(g.f, g.off+g.dropped, window)
		g.dropped = g.pos
	}
	if unpaced := g.pos - g.paced; unpaced > 0 {
		gentleSleep(time.Duration(int64(gentlePause) * unpaced / GentleWindow))
		g.paced = g.pos
	}
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
	if g.progress != nil && at < g.pos {
		if pw, ok := g.progress.(interface{ IncrBy(int64) }); ok {
			pw.IncrBy(at - g.pos)
		}
	}
	g.pos, g.dropped, g.paced = at, at, at
	return at, nil
}

// FadviseSequentialRead tells the kernel a whole file is about to be read
// straight through, when the caller asked for gentle I/O.
func FadviseSequentialRead(f *os.File, gentle bool) {
	if gentle {
		FadviseReadSequential(f)
	}
}
