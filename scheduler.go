// Copyright (c) 2021 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

// Package scheduler implements a task scheduler.
package scheduler

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// Scheduler represents a task scheduler.
type Scheduler struct {
	lock       sync.Mutex
	cond       sync.Cond
	pending    []func()
	tasks      int64
	workers    int64
	maxWorkers int64
}

// New returns a new task scheduler.
func New(maxWorkers int) *Scheduler {
	if maxWorkers == 0 {
		maxWorkers = runtime.NumCPU()
	}
	s := &Scheduler{
		maxWorkers: int64(maxWorkers),
	}
	s.cond.L = &s.lock
	return s
}

// Schedule schedules the task.
func (s *Scheduler) Schedule(task func()) {
	workers := atomic.LoadInt64(&s.workers)
	if atomic.AddInt64(&s.tasks, 1) > workers && workers < s.maxWorkers {
		if atomic.AddInt64(&s.workers, 1) <= s.maxWorkers {
			go s.worker(task)
			return
		}
	}
	s.lock.Lock()
	s.pending = append(s.pending, task)
	s.lock.Unlock()
	s.cond.Signal()
}

func (s *Scheduler) worker(task func()) {
	for {
		task()
		atomic.AddInt64(&s.tasks, -1)
		s.lock.Lock()
		for {
			if len(s.pending) > 0 {
				task = s.pending[0]
				s.pending = s.pending[1:]
				break
			}
			s.cond.Wait()
		}
		s.lock.Unlock()
	}
}
