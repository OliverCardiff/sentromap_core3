package progress

import (
	"fmt"
	"sync"
)

// ProgCount is a progress counter
type ProgCount struct {
	completed int
	updates   chan int
	name      string
	wg        *sync.WaitGroup
}

// NewProgCount returns a new progress bar to update
func NewProgCount(name string) *ProgCount {
	p := ProgCount{completed: 0, name: name}
	p.updates = make(chan int, 2)
	var wg sync.WaitGroup
	p.wg = &wg

	return &p
}

func (p *ProgCount) redraw() {

	start := "\r" + p.name + ": "
	val := fmt.Sprintf("%d", p.completed)

	print(start + val)
}

func (p *ProgCount) updater() {

	for nxt := range p.updates {
		p.completed += nxt
		p.redraw()
	}
	p.wg.Done()
}

// Update updates the progress bar and causes a redraw
func (p *ProgCount) Update(num int) {
	p.updates <- num
}

// Run commences the detacted thread which draws progress updates
func (p *ProgCount) Run() {
	p.wg.Add(1)
	go p.updater()
}

// Stop kills the update thread and stops updating progress
func (p *ProgCount) Stop() {
	close(p.updates)
	p.wg.Wait()
	println()
}
