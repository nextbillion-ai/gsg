package bar

import (
	"fmt"
	"math"
	"sync"
	"time"
)

var (
	// Pretty defines if use pretty print
	Pretty = true
)

// Container holds attributes of a bar
type Container struct {
	bars        []*ProgressBar
	refreshInMs int
	sync.Mutex
}

// ProgressBar holds attributes of a bar
type ProgressBar struct {
	Container   *Container
	Progress    int64
	Total       int64
	Speed       float64
	StartTime   time.Time
	CurrentTime time.Time
	Prepend     string
	onceStart   sync.Once
	onceEnd     sync.Once
}

// New creates a BarContainer
func New() (*Container, error) {
	b := &Container{
		bars:        []*ProgressBar{},
		refreshInMs: 50,
	}
	go b.printer()
	return b, nil
}

// GetScreenDimensions gets the dimensions of
func (b *Container) GetScreenDimensions() (cols int, lines int) {
	return 0, 0
}

// printer check bars and draw
func (b *Container) printer() {
	for {
		for _, bar := range b.bars {
			bar.drawSimple()
		}
		time.Sleep(time.Millisecond * time.Duration(b.refreshInMs))
	}
}

// New creates a ProgressBar
func (b *Container) New(total int64, prepend string) *ProgressBar {
	bar := &ProgressBar{
		Container:   b,
		Progress:    0,
		Total:       total,
		Speed:       0,
		StartTime:   time.Now(),
		CurrentTime: time.Now(),
		Prepend:     prepend,
	}

	// protect here, possible data racing as printer access bars in goroutine
	b.Lock()
	b.bars = append(b.bars, bar)
	b.Unlock()

	// init bar
	bar.drawSimple()
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
	//return
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
