package common

import "sync"

// ParallelDo calls fn for every index in [0, n), with at most limit calls
// running at once, and returns once all of them have completed.
//
// A limit below one is treated as one. A limit above n is capped at n, so no
// more goroutines are started than there is work.
func ParallelDo(n, limit int, fn func(index int)) {
	if n <= 0 {
		return
	}
	if limit <= 0 {
		limit = 1
	}
	if limit > n {
		limit = n
	}

	var wg sync.WaitGroup
	// Acquiring before the goroutine is started is what bounds this: the loop
	// blocks here rather than queueing up n goroutines.
	sem := make(chan struct{}, limit)
	for i := 0; i < n; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(index)
		}(i)
	}
	wg.Wait()
}
