// Copyright (c) 2021 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

// Package scheduler implements a task scheduler.
package scheduler

import (
	"runtime"
	"time"
)

const idle = time.Second

// Scheduler represents a task scheduler.
type Scheduler struct {
	idleTime time.Duration
	tasks    chan func()
	workers  chan struct{}
}

// New returns a new task scheduler.
func New(maxWorkers int, idleTime time.Duration) *Scheduler {
	if maxWorkers == 0 {
		maxWorkers = runtime.NumCPU()
	}
	if idleTime == 0 {
		idleTime = idle
	}
	s := &Scheduler{
		idleTime: idleTime,
		tasks:    make(chan func()),
		workers:  make(chan struct{}, maxWorkers),
	}
	return s
}

// Schedule schedules the task.
func (s *Scheduler) Schedule(task func()) {
	select {
	case s.tasks <- task:
	case s.workers <- struct{}{}:
		go s.worker(task)
	default:
		go task()
	}
}

func (s *Scheduler) worker(task func()) {
	defer func() { <-s.workers }()
	for {
		task()
		t := time.NewTimer(s.idleTime)
		select {
		case task = <-s.tasks:
			t.Stop()
		case <-t.C:
			return
		}
	}
}
