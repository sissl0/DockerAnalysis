package database

import "container/heap"

type FragEntry struct {
	hash      uint64
	hasSecret bool
	score     uint32
	index     int // Position im Heap
}

type FragMinHeap []*FragEntry

type FragCache struct {
	capacity int
	entries  map[uint64]*FragEntry
	h        FragMinHeap
}

func (h FragMinHeap) Len() int           { return len(h) }
func (h FragMinHeap) Less(i, j int) bool { return h[i].score < h[j].score }
func (h FragMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *FragMinHeap) Push(x interface{}) {
	e := x.(*FragEntry)
	e.index = len(*h)
	*h = append(*h, e)
}
func (h *FragMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	e.index = -1
	*h = old[:n-1]
	return e
}

func NewFragCache(cap int) *FragCache {
	if cap <= 0 {
		cap = 1
	}
	return &FragCache{
		capacity: cap,
		entries:  make(map[uint64]*FragEntry, cap/2),
		h:        make(FragMinHeap, 0, cap),
	}
}

func (c *FragCache) Get(h uint64) (hasSecret bool, ok bool) {
	e, ok := c.entries[h]
	if !ok {
		return false, false
	}
	inc := uint32(1)
	if e.hasSecret {
		inc += 4
	}
	e.score += inc
	heap.Fix(&c.h, e.index)
	return e.hasSecret, true
}

func (c *FragCache) Set(h uint64, hasSecret bool) {
	if e, ok := c.entries[h]; ok {
		if hasSecret && !e.hasSecret {
			e.hasSecret = true
			e.score += 5
		} else {
			e.score++
		}
		heap.Fix(&c.h, e.index)
		return
	}
	if c.capacity > 0 && len(c.entries) >= c.capacity {
		ev := heap.Pop(&c.h).(*FragEntry)
		delete(c.entries, ev.hash)
	}
	score := uint32(1)
	if hasSecret {
		score = 6
	}
	e := &FragEntry{
		hash:      h,
		hasSecret: hasSecret,
		score:     score,
	}
	heap.Push(&c.h, e)
	c.entries[h] = e
}
