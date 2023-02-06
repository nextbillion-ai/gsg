package worker

import (
	"gsutil-go/common"
	"gsutil-go/logger"
	"sync"
	"time"
)

// Pool is the worker pool
type Pool struct {
	enableLog bool
	size      int
	jc        chan func()
	wg        *sync.WaitGroup
}

func (p *Pool) log(s string, vs ...any) {
	if p.enableLog {
		logger.Debug(s, vs...)
	}
}

// Add adds job into pool
func (p *Pool) Add(job func()) {
	p.jc <- job
	p.log("added job to pool")
}

// Run runs all jobs with worker pool
func (p *Pool) Run() {
	p.log("starting workers with %d workers", p.size)
	for i := 0; i < p.size; i++ {
		p.wg.Add(1)
		go p.worker(i, p.jc, p.wg)
	}
}

// Close closes all workers
func (p *Pool) Close() {
	close(p.jc)
	p.wg.Wait()
	p.log("finished all the jobs")
}

func (p *Pool) worker(id int, jc <-chan func(), wg *sync.WaitGroup) {
	defer common.Recovery()
	defer wg.Done()

	start := time.Now()
	p.log("started worker %d", id)
	for job := range jc {
		job()
	}
	p.log("stopped worker %d, with %s", id, time.Since(start))
}

// New creates a worker pool with size
func New(size int, enableLog bool) *Pool {
	if size <= 0 {
		size = 1 // should at least one worker
	}
	p := &Pool{
		enableLog: enableLog,
		size:      size,
		jc:        make(chan func(), size),
		wg:        new(sync.WaitGroup),
	}
	p.log("created pool with size %d", size)
	return p
}
