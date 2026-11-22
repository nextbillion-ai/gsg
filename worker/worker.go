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
	wgs       []*sync.WaitGroup
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
	for depth, jc := range p.jcs {
		p.wgs[depth].Add(p.size)
		for i := 0; i < p.size; i++ {
			go p.worker(i, jc, p.wgs[depth])
		}
	}
}

// Close closes the depths in order, each one only after the workers of the
// depth before it have finished: a job still running there may submit deeper.
func (p *Pool) Close() {
	for depth, jc := range p.jcs {
		close(jc)
		p.wgs[depth].Wait()
	}
	p.log("finished all the jobs")
}

func (p *Pool) worker(id int, jc <-chan func(), wg *sync.WaitGroup) {
	defer wg.Done()

	start := time.Now()
	p.log("started worker %d", id)
	for job := range jc {
		p.run(job)
	}
	p.log("stopped worker %d, with %s", id, time.Since(start))
}

// run recovers each job on its own. The command exits on a panic, as it always
// has; a library caller's process is not gsg's to end, so there the panic is
// logged and the worker goes on to the next job instead of dying and leaving
// the pool a worker short.
func (p *Pool) run(job func()) {
	defer common.Recovery()
	job()
}

// Submitter returns what a transfer hands its chunks to.
//
// With a pool the chunks go to it at depth 1, where the gs and s3 transfers
// have always put theirs, so a transfer running as a depth 0 job never waits
// on its own chunks. Without one -- a library caller's RunContext need not
// carry a pool -- each chunk gets a goroutine, at most bound at once, or
// common.PartConcurrency(chunks) when bound is below 1. The slot is taken
// before the goroutine starts: the chunk count follows the object, and 80 GiB
// at a 1 MiB chunk size is 81920 goroutines parked before the first byte moves.
//
// Waiting for the chunks is the caller's, and so is recovering a panic in one:
// only the caller knows how to fail the transfer it belongs to.
func Submitter(pool *Pool, chunks, bound int) func(job func()) {
	if pool != nil {
		return func(job func()) { pool.AddWithDepth(1, job) }
	}
	if bound < 1 {
		bound = common.PartConcurrency(int64(chunks))
	}
	if bound < 1 {
		bound = 1
	}
	sem := make(chan struct{}, bound)
	return func(job func()) {
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()
			job()
		}()
	}
}

// RunChunks runs job once for each of chunks through Submitter and waits for
// all of them.
func RunChunks(pool *Pool, chunks, bound int, job func(i int)) {
	var wg sync.WaitGroup
	submit := Submitter(pool, chunks, bound)
	for i := 0; i < chunks; i++ {
		i := i
		wg.Add(1)
		submit(func() {
			defer wg.Done()
			job(i)
		})
	}
	wg.Wait()
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
	}
	p.jcs = make([]chan func(), depth)
	p.wgs = make([]*sync.WaitGroup, depth)
	for index := range p.jcs {
		p.jcs[index] = make(chan func())
		p.wgs[index] = new(sync.WaitGroup)
	}
	p.log("created pool with size %d", size)
	return p
}
