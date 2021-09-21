package pqutil

// pq implements the required interface to be used as a heap data structure using 'container/heap'.
type pq []Item

func (p pq) Len() int {
	return len(p)
}

func (p pq) Less(i, j int) bool {
	return p[i].Priority > p[j].Priority
}

func (p pq) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p *pq) Push(x interface{}) {
	*p = append(*p, x.(Item))
}

func (p *pq) Pop() interface{} {
	x := (*p)[len(*p)-1]
	(*p) = (*p)[:len(*p)-1]

	return x
}
