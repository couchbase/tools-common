package pqutil

// pq implements the required interface to be used as a heap data structure using 'container/heap'.
type pq[T any] []Item[T]

func (p pq[T]) Len() int {
	return len(p)
}

func (p pq[T]) Less(i, j int) bool {
	return p[i].Priority > p[j].Priority
}

func (p pq[T]) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p *pq[T]) Push(x interface{}) {
	*p = append(*p, x.(Item[T]))
}

func (p *pq[T]) Pop() interface{} {
	x := (*p)[len(*p)-1]
	(*p) = (*p)[:len(*p)-1]

	return x
}
