# scheduler
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
	"time"
)

func main() {
	p := scheduler.New(64, time.Second)
	wg := &sync.WaitGroup{}
	for i := 0; i < 100000; i++ {
		wg.Add(1)
		task := func() {
			wg.Done()
		}
		p.Schedule(task)
	}
	wg.Wait()
}
```

### License
This package is licensed under a MIT license (Copyright (c) 2021 Meng Huang)


### Author
scheduler was written by Meng Huang.