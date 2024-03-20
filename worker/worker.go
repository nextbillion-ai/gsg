package worker

import (
	"sync"
	"time"

	"github.com/nextbillion-ai/gsg/common"
	"github.com/nextbillion-ai/gsg/logger"
)

// Pool is the worker pool
type Pool struct {
	enableLog bool
	size      int
	jcs       []chan func()
	wg        *sync.WaitGroup
}

func (p *Pool) log(s string, vs ...any) {
	if p.enableLog {
		logger.Debug("pool", s, vs...)
	}
}

func (p *Pool) Add(job func()) {
	p.AddWithDepth(0, job)
}

func (p *Pool) AddWithDepth(depth int, job func()) {
	p.jcs[depth] <- job
	p.log("added job to pool")
}

// Run runs all jobs with worker pool
func (p *Pool) Run() {
	p.log("starting workers with %d workers", p.size)
	for i := 0; i < p.size; i++ {
		p.wg.Add(len(p.jcs))
		for _, jc := range p.jcs {
			go p.worker(i, jc, p.wg)
		}
	}
}

// Close closes all workers
func (p *Pool) Close() {
	for _, jc := range p.jcs {
		close(jc)
	}
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

func New(size int, enableLog bool) *Pool {
	return NewWithDepth(size, 2, enableLog)
}

func NewWithDepth(size, depth int, enableLog bool) *Pool {
	if size <= 0 {
		size = 1 // should at least one worker
	}
	p := &Pool{
		enableLog: enableLog,
		size:      size,
		wg:        new(sync.WaitGroup),
	}
	p.jcs = make([]chan func(), depth)
	for index := range p.jcs {
		p.jcs[index] = make(chan func())
	}
	p.log("created pool with size %d", size)
	return p
}
