# scheduler
[![PkgGoDev](https://pkg.go.dev/badge/github.com/hslam/scheduler)](https://pkg.go.dev/github.com/hslam/scheduler)
[![Build Status](https://github.com/hslam/scheduler/workflows/build/badge.svg)](https://github.com/hslam/scheduler/actions)
[![codecov](https://codecov.io/gh/hslam/scheduler/branch/master/graph/badge.svg)](https://codecov.io/gh/hslam/scheduler)
[![Go Report Card](https://goreportcard.com/badge/github.com/hslam/scheduler)](https://goreportcard.com/report/github.com/hslam/scheduler)
[![LICENSE](https://img.shields.io/github/license/hslam/scheduler.svg?style=flat-square)](https://github.com/hslam/scheduler/blob/master/LICENSE)

Package scheduler implements a task scheduler.

## Get started

### Install
```
go get github.com/hslam/scheduler
```
### Import
```
import "github.com/hslam/scheduler"
```
### Usage
#### Example
```go
package main

import (
	"github.com/hslam/scheduler"
	"sync"
)

func main() {
	s := scheduler.New(64, nil)
	wg := &sync.WaitGroup{}
	for i := 0; i < 100000; i++ {
		wg.Add(1)
		task := func() {
			wg.Done()
		}
		s.Schedule(task)
	}
	wg.Wait()
	s.Close()
}
```

### License
This package is licensed under a MIT license (Copyright (c) 2021 Meng Huang)


### Author
scheduler was written by Meng Huang.