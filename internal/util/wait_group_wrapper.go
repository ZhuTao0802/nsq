package util

import (
	"sync"
)

// WaitGroup封装
type WaitGroupWrapper struct {
	sync.WaitGroup
}

// 封装一个func()，让其异步执行
func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
