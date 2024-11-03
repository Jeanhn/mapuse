package util

import "container/list"

type Queue struct {
	queue *list.List
}

func NewQueue() Queue {
	return Queue{
		queue: list.New(),
	}
}

func (q *Queue) Push(e interface{}) {
	q.queue.PushBack(e)
}

func (q *Queue) Pop() interface{} {
	v := q.queue.Front().Value
	q.queue.Remove(q.queue.Front())
	return v
}

func (q *Queue) Empty() bool {
	return q.queue.Len() == 0
}
