package data

import "testing"

func TestDataAndCounter(t *testing.T) {
	var counter Counter
	df := DefaultCounter()
	err := Default().Put("1", df)
	if err != nil {
		t.Error(err)
	}
	err, ok := Default().Get("1", &counter)
	if err != nil {
		t.Error(err)
	}
	if !ok {
		t.Error("not ok")
	}
	counter.Add()
	i, _ := counter.Get()
	if i != 1 {
		t.Fatal("ft")
	}
}
