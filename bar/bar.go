package bar

import (
	"fmt"
	"gsutil-go/logger"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sethgrid/curse"
	"golang.org/x/sys/unix"
)

const (
	reservedPadding = 80
)

var (
	// Pretty defines if use pretty print
	Pretty      = true
	_isatty     = false
	_isattyOnce sync.Once
)

// Container holds attributes of a bar container
type Container struct {
	bars                    []*ProgressBar
	refreshInMs             int
	screenLines             int
	screenWidth             int
	startingLine            int
	totalNewlines           int
	historicNewlinesCounter int
	history                 map[int]string
	sync.Mutex
}

// ProgressBar holds attributes of a bar
type ProgressBar struct {
	Container   *Container
	Width       int
	Progress    int64
	Total       int64
	Speed       float64
	LeftEnd     string
	RightEnd    string
	Fill        string
	Head        string
	Empty       string
	StartTime   time.Time
	CurrentTime time.Time
	Line        int
	Prepend     string
	onceStart   sync.Once
	onceEnd     sync.Once
}

// New creates a BarContainer
func New() (*Container, error) {
	width, lines, _ := curse.GetScreenDimensions()
	if width <= 0 {
		width = 1
	}
	if lines <= 0 {
		lines = 1
	}
	logger.Debug("screen dimension with %d width, %d lines", width, lines)
	line := 0
	if Pretty && isatty() {
		_, line, _ = curse.GetCursorPosition()
	}
	history := make(map[int]string)
	b := &Container{
		bars:         []*ProgressBar{},
		refreshInMs:  50,
		screenWidth:  width,
		screenLines:  lines,
		startingLine: line,
		history:      history,
	}
	go b.printer()
	return b, nil
}

// GetScreenDimensions gets the dimensions of
func (b *Container) GetScreenDimensions() (cols int, lines int) {
	return b.screenWidth, b.screenLines
}

// printer check bars and draw
func (b *Container) printer() {
	for {
		for _, bar := range b.bars {
			if Pretty && isatty() {
				bar.draw()
			} else {
				bar.drawSimple()
			}
		}
		time.Sleep(time.Millisecond * time.Duration(b.refreshInMs))
	}
}

// New creates a ProgressBar
func (b *Container) New(total int64, prepend string) *ProgressBar {
	width := b.screenWidth - len(prepend) - reservedPadding
	if width < 0 {
		width = 0
	}
	bar := &ProgressBar{
		Container:   b,
		Width:       width,
		Progress:    0,
		Total:       total,
		Speed:       0,
		LeftEnd:     "|",
		RightEnd:    "|",
		Fill:        "█",
		Head:        "█",
		Empty:       "▒",
		StartTime:   time.Now(),
		CurrentTime: time.Now(),
		Line:        b.startingLine + b.totalNewlines,
		Prepend:     prepend,
	}

	// protect here, possible data racing as printer access bars in goroutine
	b.Lock()
	b.bars = append(b.bars, bar)
	b.history[bar.Line] = ""
	b.Unlock()

	// init bar
	if Pretty && isatty() {
		bar.draw()
		_, _ = b.Println()
	} else {
		bar.drawSimple()
	}
	return bar
}

// IncrBy increate progress by an number
func (p *ProgressBar) IncrBy(delta int64) {
	p.CurrentTime = time.Now()
	p.Progress += delta
	if p.Progress > p.Total {
		p.Progress = p.Total
	}
	p.Speed = 0
	elapsed := p.CurrentTime.Sub(p.StartTime).Seconds()
	if elapsed > 0 {
		p.Speed = float64(p.Progress) / elapsed
	}
}

// Write implement io.Writer
func (p *ProgressBar) Write(bs []byte) (n int, err error) {
	n = len(bs)
	if n > 0 {
		p.IncrBy(int64(n))
	}
	bs = nil
	return
}

// draw simple sentences, suitable for non-tty interface
func (p *ProgressBar) drawSimple() {
	p.onceStart.Do(func() {
		fmt.Printf("%s In progress\n", p.Prepend)
	})
	if p.Progress >= p.Total {
		p.onceEnd.Do(func() {
			elapsed := fmt.Sprintf("%d", int64(p.CurrentTime.Sub(p.StartTime).Seconds())) + "s"
			fmt.Printf(
				"%s Done (%s, %s, %s)\n",
				p.Prepend,
				humanizeBytes(float64(p.Total)), // total
				elapsed,                         // elapsed
				humanizeBytes(p.Speed)+"/s",     // speed
			)
		})
	}
	return
}

// draw with aninmation effect
func (p *ProgressBar) draw() {
	p.Container.Lock()
	defer p.Container.Unlock()

	// draw bar
	justGotToFirstEmptySpace := true
	bar := make([]string, p.Width)
	for i := range bar {
		if float32(p.Progress)/float32(p.Total) > float32(i)/float32(p.Width) {
			bar[i] = p.Fill
		} else {
			bar[i] = p.Empty
			if justGotToFirstEmptySpace {
				bar[i] = p.Head
				justGotToFirstEmptySpace = false
			}
		}
	}

	// draw other fields
	rate := math.Ceil(100 * (float64(p.Progress) / float64(p.Total)))
	if rate > 100 {
		rate = 100
	}
	percentage := strconv.Itoa(int(rate)) + "%"
	elapsed := fmt.Sprintf("%d", int64(p.CurrentTime.Sub(p.StartTime).Seconds())) + "s"
	eta := "-"
	if p.Speed > 0 {
		eta = fmt.Sprintf("%d", int64(float64(p.Total-p.Progress)/float64(p.Speed))) + "s"
	}

	// erase and print
	c, _ := curse.New()
	c.Move(1, p.Line)
	c.EraseCurrentLine()
	fmt.Printf(
		"\r%s %3s %s%s%s (%s/%s, %s/%s, %s)",
		p.Prepend,                          // description
		percentage,                         // percentage
		p.LeftEnd,                          // bar
		strings.Join(bar, ""),              // bar
		p.RightEnd,                         // bar
		humanizeBytes(float64(p.Progress)), // progress
		humanizeBytes(float64(p.Total)),    // total
		elapsed,                            // elapsed
		eta,                                // eta
		humanizeBytes(p.Speed)+"/s",        // speed
	)
	c.Move(c.StartingPosition.X, c.StartingPosition.Y)
}

func (b *Container) addedNewlines(count int) {
	b.totalNewlines += count
	b.historicNewlinesCounter += count

	// if we hit the bottom of the screen, we "scroll" our bar displays by pushing
	// them up count lines (closer to line 0)
	if b.startingLine+b.totalNewlines > b.screenLines {
		b.totalNewlines -= count
		for _, bar := range b.bars {
			bar.Line -= count
		}
		b.redrawAll(count)
	}
}

func (b *Container) redrawAll(moveUp int) {
	c, _ := curse.New()

	newHistory := make(map[int]string)
	for line, printed := range b.history {
		newHistory[line+moveUp] = printed
		c.Move(1, line)
		c.EraseCurrentLine()
		c.Move(1, line+moveUp)
		c.EraseCurrentLine()
		fmt.Print(printed)
	}
	b.history = newHistory
	c.Move(c.StartingPosition.X, c.StartingPosition.Y)
}

// Print prints wrappers to capture newlines to adjust line positions on bars
func (b *Container) Print(a ...interface{}) (n int, err error) {
	b.Lock()
	defer b.Unlock()

	newlines := countAllNewlines(a...)
	b.addedNewlines(newlines)
	thisLine := b.startingLine + b.totalNewlines
	b.history[thisLine] = fmt.Sprint(a...)
	return fmt.Print(a...)
}

// Printf prints wrappers to capture newlines to adjust line positions on bars
func (b *Container) Printf(format string, a ...interface{}) (n int, err error) {
	b.Lock()
	defer b.Unlock()

	newlines := strings.Count(format, "\n")
	newlines += countAllNewlines(a...)
	b.addedNewlines(newlines)
	thisLine := b.startingLine + b.totalNewlines
	b.history[thisLine] = fmt.Sprintf(format, a...)
	return fmt.Printf(format, a...)
}

// Println prints wrappers to capture newlines to adjust line positions on bars
func (b *Container) Println(a ...interface{}) (n int, err error) {
	b.Lock()
	defer b.Unlock()

	newlines := countAllNewlines(a...) + 1
	b.addedNewlines(newlines)
	thisLine := b.startingLine + b.totalNewlines
	b.history[thisLine] = fmt.Sprint(a...)
	return fmt.Println(a...)
}

func countAllNewlines(interfaces ...interface{}) int {
	count := 0
	for _, iface := range interfaces {
		switch s := iface.(type) {
		case string:
			count += strings.Count(s, "\n")
		}
	}
	return count
}

func humanizeBytes(s float64) string {
	sizes := []string{"B", "kB", "MB", "GB", "TB", "PB", "EB"}
	base := 1024.0
	if s < 10 {
		return fmt.Sprintf("%2.0f%s", s, sizes[0])
	}
	e := math.Floor(math.Log(float64(s)) / math.Log(base))
	suffix := sizes[int(e)]
	val := math.Floor(float64(s)/math.Pow(base, e)*10+0.5) / 10
	return fmt.Sprintf("%.1f%s", val, suffix)
}

func isatty() bool {
	_isattyOnce.Do(func() {
		_, err := unix.IoctlGetWinsize(int(os.Stdout.Fd()), unix.TIOCGWINSZ)
		if err != nil {
			_isatty = false
		}
		_isatty = true
	})
	return _isatty
}

// func main() {
// 	var limit int64 = 1024 * 1024 * 500
// 	progressBars, _ := New()
// 	bar1 := progressBars.New(100, "1st - test:")
// 	bar2 := progressBars.New(100, "2nd - with description:")
// 	bar3 := progressBars.New(limit, "3rd - from file:")

// 	wg := new(sync.WaitGroup)

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for i := 0; i < 100; i++ {
// 			bar1.IncrBy(1)
// 			time.Sleep(time.Millisecond * 15)
// 		}
// 	}()

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for i := 0; i < 100; i++ {
// 			bar2.IncrBy(1)
// 			time.Sleep(time.Millisecond * 50)
// 		}
// 	}()

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		reader := io.LimitReader(rand.Reader, limit/4)
// 		_, _ = io.Copy(bar3, reader)
// 	}()

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		reader := io.LimitReader(rand.Reader, limit/4)
// 		_, _ = io.Copy(bar3, reader)
// 	}()

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		reader := io.LimitReader(rand.Reader, limit/4)
// 		_, _ = io.Copy(bar3, reader)
// 	}()

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		reader := io.LimitReader(rand.Reader, limit/4)
// 		_, _ = io.Copy(bar3, reader)
// 	}()

// 	wg.Wait()
// 	fmt.Println("Done")
// }
