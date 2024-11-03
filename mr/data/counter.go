package data

import "sync/atomic"

type Counter interface {
	Add() (int, error)
	Get() (int, error)
}

type defaultCounter struct {
	i int64
}

func (dc *defaultCounter) Add() (int, error) {
	return int(atomic.AddInt64(&dc.i, 1)) - 1, nil
}

func (dc *defaultCounter) Get() (int, error) {
	return int(atomic.LoadInt64(&dc.i)), nil
}

var defaultGlobalCounter Counter = &defaultCounter{
	i: 0,
}

func DefaultCounter() Counter {
	return defaultGlobalCounter
}
