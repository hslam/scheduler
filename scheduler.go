// Copyright (c) 2021 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

// Package scheduler implements a task scheduler.
package scheduler

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	threshold = 1
	idleTime  = time.Second
	interval  = time.Second

	// Unlimited represents the unlimited number of goroutines.
	Unlimited = 0
)

// Scheduler represents a task scheduler.
type Scheduler interface {
	// Schedule schedules the task to an idle worker.
	Schedule(task func())
	// NumWorkers returns the number of workers.
	NumWorkers() int
	// Close closes the task scheduler.
	Close()
}

// Options represents options
type Options struct {
	// Threshold option represents the threshold at which batch task is enabled.
	// If the threshold option is greater than 1, tasks will be scheduled in batches.
	Threshold int
	// IdleTime option represents the max idle time of the worker.
	IdleTime time.Duration
	// Interval option represents the check interval of the worker.
	Interval time.Duration
}

// DefaultOptions returns default options.
func DefaultOptions() *Options {
	return &Options{
		Threshold: threshold,
		IdleTime:  idleTime,
		Interval:  interval,
	}
}

type scheduler struct {
	lock       sync.Mutex
	cond       sync.Cond
	wg         sync.WaitGroup
	pending    []func()
	tasks      int64
	running    map[*worker]struct{}
	workers    int64
	maxWorkers int64
	close      chan struct{}
	closed     int32
	opts       Options
}

// New returns a new task scheduler.
func New(maxWorkers int, opts *Options) Scheduler {
	// If the max number of workers is not greater than zero, the number of goroutines will not be limited.
	if maxWorkers <= 0 {
		maxWorkers = Unlimited
	}
	if opts == nil {
		opts = DefaultOptions()
	} else {
		if opts.IdleTime <= 0 {
			opts.IdleTime = idleTime
		}
		if opts.Interval <= 0 {
			opts.Interval = interval
		}
	}
	s := &scheduler{
		running:    make(map[*worker]struct{}),
		maxWorkers: int64(maxWorkers),
		close:      make(chan struct{}),
		opts:       *opts,
	}
	s.cond.L = &s.lock
	s.wg.Add(1)
	go s.run()
	return s
}

// Schedule schedules the task to an idle worker.
func (s *scheduler) Schedule(task func()) {
	if atomic.LoadInt32(&s.closed) > 0 {
		panic("schedule tasks on a closed scheduler")
	}
	atomic.AddInt64(&s.tasks, 1)
	for {
		workers := atomic.LoadInt64(&s.workers)
		if atomic.LoadInt64(&s.tasks) > workers && (s.maxWorkers == Unlimited || workers < s.maxWorkers) {
			if s.maxWorkers == Unlimited {
				atomic.AddInt64(&s.workers, 1)
				s.runWorker(task)
				return
			} else if atomic.CompareAndSwapInt64(&s.workers, workers, workers+1) {
				s.runWorker(task)
				return
			}
		} else {
			break
		}
	}
	s.lock.Lock()
	s.pending = append(s.pending, task)
	s.lock.Unlock()
	s.cond.Signal()
}

func (s *scheduler) runWorker(task func()) {
	s.wg.Add(1)
	w := &worker{}
	s.lock.Lock()
	s.running[w] = struct{}{}
	s.lock.Unlock()
	go w.run(s, task)
}

// NumWorkers returns the number of workers.
func (s *scheduler) NumWorkers() int {
	return int(atomic.LoadInt64(&s.workers))
}

// Close closes the task scheduler.
func (s *scheduler) Close() {
	if atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		close(s.close)
	}
	s.wg.Wait()
}

func (s *scheduler) run() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.opts.Interval)
	var done bool
	var idle bool
	var lastIdleTime time.Time
	for {
		if !done {
			select {
			case <-ticker.C:
				if atomic.LoadInt64(&s.workers) > 0 && atomic.LoadInt64(&s.workers) > atomic.LoadInt64(&s.tasks) {
					if !idle {
						idle = true
						lastIdleTime = time.Now()
					} else if time.Now().Sub(lastIdleTime) > s.opts.IdleTime {
						s.lock.Lock()
						deletions := len(s.running) - len(s.pending)
						if deletions > 4 {
							deletions = deletions / 4
						} else if deletions > 0 {
							deletions = 1
						}
						if deletions > 0 {
							for w := range s.running {
								delete(s.running, w)
								w.close()
								deletions--
								if deletions == 0 {
									break
								}
							}
						}
						s.lock.Unlock()
						s.cond.Broadcast()
						idle = false
						lastIdleTime = time.Time{}
					}
				} else {
					idle = false
					lastIdleTime = time.Time{}
				}
			case <-s.close:
				ticker.Stop()
				done = true
			}
		} else {
			if atomic.LoadInt64(&s.workers) == 0 {
				break
			}
			s.cond.Broadcast()
			time.Sleep(time.Millisecond)
		}
	}
}

type worker struct {
	closed int32
}

func (w *worker) run(s *scheduler, task func()) {
	var maxWorkers = int(s.maxWorkers)
	var batch []func()
	for {
		if len(batch) > 0 {
			for _, task = range batch {
				task()
			}
			atomic.AddInt64(&s.tasks, -int64(len(batch)))
			batch = batch[:0]
		} else {
			task()
			atomic.AddInt64(&s.tasks, -1)
		}
		s.lock.Lock()
		for {
			if maxWorkers != Unlimited && s.opts.Threshold > 1 && len(s.pending) > maxWorkers*s.opts.Threshold {
				alloc := len(s.pending) / maxWorkers
				batch = s.pending[:alloc]
				s.pending = s.pending[alloc:]
				break
			} else if len(s.pending) > 0 {
				task = s.pending[0]
				s.pending = s.pending[1:]
				break
			}
			s.cond.Wait()
			if atomic.LoadInt32(&s.closed) > 0 || atomic.LoadInt32(&w.closed) > 0 {
				s.lock.Unlock()
				atomic.AddInt64(&s.workers, -1)
				s.wg.Done()
				s.cond.Broadcast()
				return
			}
		}
		s.lock.Unlock()
	}
}

func (w *worker) close() {
	atomic.CompareAndSwapInt32(&w.closed, 0, 1)
}
