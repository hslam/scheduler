// Copyright (c) 2021 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

// Package scheduler implements a task scheduler.
package scheduler

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	threshold  = 1
	idleTime   = time.Second
	interval   = time.Second
	numBuckets = 256

	// Unlimited represents the unlimited number of goroutines.
	Unlimited = 0
)

var (
	buckets = func() [numBuckets]*scheduler {
		var buckets = [numBuckets]*scheduler{}
		for i := 0; i < numBuckets; i++ {
			buckets[i] = newScheduler(Unlimited, nil)
		}
		return buckets
	}()
	seq uint64
)

// Schedule schedules the task to an idle worker.
func Schedule(task func()) {
	if task != nil {
		// Hashing tasks into multiple schedulers only works in a single thread.
		seq++
		buckets[seq%numBuckets].Schedule(task)
	}
}

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
		Interval:  interval,
	}
}

type schedulers struct {
	sync.RWMutex
	wake   bool
	active map[*scheduler]struct{}
}

var m = &schedulers{
	active: make(map[*scheduler]struct{}),
}

func (m *schedulers) register(s *scheduler) {
	m.Lock()
	m.active[s] = struct{}{}
	m.Unlock()
	m.wakeCheck()
}

func (m *schedulers) unregister(s *scheduler) {
	m.Lock()
	delete(m.active, s)
	m.Unlock()
}

func (m *schedulers) numSchedulers() int {
	m.RLock()
	num := len(m.active)
	m.RUnlock()
	return num
}

func (m *schedulers) wakeCheck() {
	m.Lock()
	if !m.wake {
		m.wake = true
		go func() {
			ticker := time.NewTicker(interval / 10)
			var checks []*scheduler
			for {

				select {
				case <-ticker.C:
					now := time.Now()
					m.RLock()
					for s := range m.active {
						if now.Sub(s.lastCheckTime) > s.opts.Interval {
							s.lastCheckTime = now
							checks = append(checks, s)
						}
					}
					m.RUnlock()
					for i, s := range checks {
						checks[i] = nil
						s.check()
					}
					checks = checks[:0]
					m.Lock()
					if len(m.active) == 0 {
						m.wake = false
						ticker.Stop()
						m.Unlock()
						return
					}
					m.Unlock()
				}
				runtime.Gosched()
			}
		}()
	}
	m.Unlock()
}

type scheduler struct {
	lock          sync.RWMutex
	cond          sync.Cond
	wg            sync.WaitGroup
	pending       []func()
	tasks         int64
	running       map[*worker]struct{}
	workers       int
	maxWorkers    int
	lastIdleTime  time.Time
	lastCheckTime time.Time
	closed        int32
	opts          Options
}

// New returns a new task scheduler.
func New(maxWorkers int, opts *Options) Scheduler {
	return newScheduler(maxWorkers, opts)
}

func newScheduler(maxWorkers int, opts *Options) *scheduler {
	// If the max number of workers is not greater than zero, the number of goroutines will not be limited.
	if maxWorkers <= 0 {
		maxWorkers = Unlimited
	}
	if opts == nil {
		opts = DefaultOptions()
	} else {
		if opts.Interval <= 0 {
			opts.Interval = interval
		}
	}
	s := &scheduler{
		maxWorkers: maxWorkers,
		opts:       *opts,
	}
	if s.opts.IdleTime > 0 {
		s.running = make(map[*worker]struct{})
		s.cond.L = &s.lock
		m.register(s)
	}
	return s
}

// Schedule schedules the task to an idle worker.
func (s *scheduler) Schedule(task func()) {
	if atomic.LoadInt32(&s.closed) > 0 {
		if task != nil {
			task()
		}
		return
	}
	tasks := atomic.AddInt64(&s.tasks, 1)
	s.lock.Lock()
	if int(tasks) > s.workers && (s.maxWorkers == Unlimited || s.workers < s.maxWorkers) {
		w := &worker{}
		if s.opts.IdleTime > 0 {
			s.running[w] = struct{}{}
		}
		s.workers++
		s.wg.Add(1)
		go w.run(s, task)
		s.lock.Unlock()
	} else {
		s.pending = append(s.pending, task)
		s.lock.Unlock()
		if s.opts.IdleTime > 0 {
			s.cond.Signal()
		}
	}
}

// NumWorkers returns the number of workers.
func (s *scheduler) NumWorkers() int {
	s.lock.RLock()
	numWorkers := s.workers
	s.lock.RUnlock()
	return numWorkers
}

// Close closes the task scheduler.
func (s *scheduler) Close() {
	if atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		s.lock.Lock()
		if s.opts.IdleTime > 0 {
			for w := range s.running {
				w.close()
			}
			s.running = make(map[*worker]struct{})
		}
		for _, task := range s.pending {
			task()
		}
		s.pending = s.pending[:0]
		s.lock.Unlock()
		if s.opts.IdleTime > 0 {
			s.cond.Broadcast()
			for {
				if s.NumWorkers() == 0 {
					break
				}
				s.cond.Broadcast()
				time.Sleep(time.Millisecond)
			}
			m.unregister(s)
		}
	}
	s.wg.Wait()
}

func (s *scheduler) check() {
	s.lock.Lock()
	if len(s.running) > 0 && len(s.running) > len(s.pending) {
		if s.lastIdleTime.IsZero() {
			s.lastIdleTime = time.Now()
		} else if time.Since(s.lastIdleTime) > s.opts.IdleTime {
			deletions := len(s.running) - len(s.pending)
			for w := range s.running {
				delete(s.running, w)
				w.close()
				deletions--
				if deletions == 0 {
					break
				}
			}
			s.lastIdleTime = time.Time{}
			s.cond.Broadcast()
		}
	} else {
		s.lastIdleTime = time.Time{}
	}
	s.lock.Unlock()
}

type worker struct {
	closed int32
}

func (w *worker) run(s *scheduler, task func()) {
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
			if s.maxWorkers != Unlimited && s.opts.Threshold > 1 && len(s.pending) > s.maxWorkers*s.opts.Threshold {
				alloc := len(s.pending) / s.maxWorkers
				batch = s.pending[:alloc]
				s.pending = s.pending[alloc:]
				break
			} else if len(s.pending) > 0 {
				task = s.pending[0]
				s.pending = s.pending[1:]
				break
			}
			if s.opts.IdleTime <= 0 {
				s.workers--
				s.lock.Unlock()
				s.wg.Done()
				return
			}
			s.cond.Wait()
			if atomic.LoadInt32(&s.closed) > 0 || atomic.LoadInt32(&w.closed) > 0 {
				s.workers--
				s.lock.Unlock()
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
