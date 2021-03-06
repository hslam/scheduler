// Copyright (c) 2021 Meng Huang (mhboy@outlook.com)
// This package is licensed under a MIT license that can be found in the LICENSE file.

package scheduler

import (
	"sync"
	"testing"
	"time"
)

func TestSchedulerPreheat(t *testing.T) {
	var (
		total       = 10000000
		concurrency = 256
	)
	opts := DefaultOptions()
	opts.Threshold = 0
	s := New(concurrency, opts)
	wg := &sync.WaitGroup{}
	for i := 0; i < total; i++ {
		wg.Add(1)
		job := func() {
			wg.Done()
		}
		s.Schedule(job)
	}
	wg.Wait()
	s.Close()
}

func TestScheduler(t *testing.T) {
	var (
		total       = 10000000
		concurrency = 1
	)
	opts := DefaultOptions()
	opts.Threshold = 0
	s := New(concurrency, opts)
	wg := &sync.WaitGroup{}
	start := time.Now()
	for i := 0; i < total; i++ {
		wg.Add(1)
		job := func() {
			wg.Done()
		}
		s.Schedule(job)
	}
	wg.Wait()
	d := time.Now().Sub(start)
	t.Logf("Concurrency:%d, Batch %t, Time:%v, %vns/op", concurrency, opts.Threshold > 1, d, int64(d)/int64(total))
	s.Close()
}

func TestSchedulerUnlimited(t *testing.T) {
	var (
		total       = 10000000
		concurrency = Unlimited
	)
	opts := DefaultOptions()
	opts.Threshold = 0
	s := New(concurrency, opts)
	wg := &sync.WaitGroup{}
	start := time.Now()
	for i := 0; i < total; i++ {
		wg.Add(1)
		job := func() {
			wg.Done()
		}
		s.Schedule(job)
	}
	wg.Wait()
	d := time.Now().Sub(start)
	t.Logf("Concurrency:unlimited, Batch %t, Time:%v, %vns/op", opts.Threshold > 1, d, int64(d)/int64(total))
	s.Close()
}

func TestSchedulerBatch(t *testing.T) {
	var (
		total       = 10000000
		concurrency = 1
	)
	opts := DefaultOptions()
	opts.Threshold = 2
	s := New(concurrency, opts)
	wg := &sync.WaitGroup{}
	start := time.Now()
	for i := 0; i < total; i++ {
		wg.Add(1)
		job := func() {
			wg.Done()
		}
		s.Schedule(job)
	}
	wg.Wait()
	d := time.Now().Sub(start)
	t.Logf("Concurrency:%d, Batch %t, Time:%v, %vns/op", concurrency, opts.Threshold > 1, d, int64(d)/int64(total))
	s.Close()
}

func TestSchedulerConcurrency(t *testing.T) {
	var (
		total       = 10000000
		concurrency = 256
	)
	opts := DefaultOptions()
	opts.Threshold = 0
	s := New(concurrency, opts)
	wg := &sync.WaitGroup{}
	start := time.Now()
	for i := 0; i < total; i++ {
		wg.Add(1)
		job := func() {
			wg.Done()
		}
		s.Schedule(job)
	}
	wg.Wait()
	d := time.Now().Sub(start)
	t.Logf("Concurrency:%d, Batch %t, Time:%v, %vns/op", concurrency, opts.Threshold > 1, d, int64(d)/int64(total))
	s.Close()
}

func TestSchedulerBatchConcurrency(t *testing.T) {
	var (
		total       = 10000000
		concurrency = 256
	)
	opts := DefaultOptions()
	opts.Threshold = 2
	s := New(concurrency, opts)
	wg := &sync.WaitGroup{}
	start := time.Now()
	for i := 0; i < total; i++ {
		wg.Add(1)
		job := func() {
			wg.Done()
		}
		s.Schedule(job)
	}
	wg.Wait()
	d := time.Now().Sub(start)
	t.Logf("Concurrency:%d, Batch %t, Time:%v, %vns/op", concurrency, opts.Threshold > 1, d, int64(d)/int64(total))
	s.Close()
}

func TestSchedulerOptions(t *testing.T) {
	{
		var (
			total       = 1000000
			concurrency = 0
		)
		s := New(concurrency, nil)
		wg := &sync.WaitGroup{}
		for i := 0; i < total; i++ {
			wg.Add(1)
			job := func() {
				wg.Done()
			}
			s.Schedule(job)
		}
		wg.Wait()
		s.Close()
	}
	{
		var (
			total       = 1000000
			concurrency = 0
		)
		opts := &Options{}
		s := New(concurrency, opts)
		wg := &sync.WaitGroup{}
		for i := 0; i < total; i++ {
			wg.Add(1)
			job := func() {
				wg.Done()
			}
			s.Schedule(job)
		}
		wg.Wait()
		s.Close()
	}
	{
		var (
			total       = 1000000
			concurrency = 1
		)
		opts := DefaultOptions()
		opts.Threshold = 2
		opts.IdleTime = time.Millisecond * 3
		opts.Interval = time.Millisecond * 1
		s := New(concurrency, opts)
		wg := &sync.WaitGroup{}
		for i := 0; i < total; i++ {
			wg.Add(1)
			job := func() {
				wg.Done()
			}
			s.Schedule(job)
		}
		wg.Wait()
		time.Sleep(interval)
		s.Close()
		time.Sleep(interval)
		if m.numSchedulers() != 0 {
			t.Error("should be 0")
		}
	}
	{
		var (
			total       = 10000
			concurrency = 64
		)
		opts := DefaultOptions()
		opts.Threshold = 2
		opts.IdleTime = time.Millisecond * 30
		opts.Interval = time.Millisecond * 10
		s := New(concurrency, opts)
		wg := &sync.WaitGroup{}
		for i := 0; i < total; i++ {
			wg.Add(1)
			job := func() {
				wg.Done()
				time.Sleep(time.Millisecond)
			}
			s.Schedule(job)
		}
		wg.Wait()
		s.Close()
	}
	func() {
		var (
			total       = 1000000
			concurrency = 0
		)
		opts := &Options{}
		s := New(concurrency, opts)
		wg := &sync.WaitGroup{}
		for i := 0; i < total; i++ {
			wg.Add(1)
			job := func() {
				wg.Done()
			}
			s.Schedule(job)
		}
		wg.Wait()
		s.Close()
		s.Schedule(func() {})
	}()
	{
		var (
			total       = 100000
			concurrency = 64
		)
		opts := &Options{}
		s := New(concurrency, opts)
		wg := &sync.WaitGroup{}
		for i := 0; i < total; i++ {
			wg.Add(1)
			job := func() {
				wg.Done()
				time.Sleep(time.Millisecond)
			}
			s.Schedule(job)
		}
		numWorkers := s.NumWorkers()
		if numWorkers != concurrency && concurrency > 0 {
			t.Errorf("NumWorkers %d != Concurrency %d", numWorkers, concurrency)
		}
		wg.Wait()
		s.Close()
	}
	{
		var (
			total       = 1024
			concurrency = 64
		)
		opts := &Options{}
		s := New(concurrency, opts)
		wg := &sync.WaitGroup{}
		for i := 0; i < total; i++ {
			wg.Add(1)
			job := func() {
				wg.Done()
				time.Sleep(time.Millisecond)
			}
			s.Schedule(job)
		}
		s.Close()
		wg.Wait()
	}
	{
		wg := &sync.WaitGroup{}
		for i := 0; i < 1024; i++ {
			wg.Add(1)
			job := func() {
				wg.Done()
			}
			Schedule(job)
		}
		wg.Wait()
	}
}
