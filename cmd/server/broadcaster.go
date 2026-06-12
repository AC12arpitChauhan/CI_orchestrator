package main

import (
	"go_ci_orchestration/core"
	"sync"
)

type Broadcaster struct {
	mu      sync.Mutex
	clients map[string]map[chan *core.Pipeline]bool
}

var broadcaster = &Broadcaster{
	clients: make(map[string]map[chan *core.Pipeline]bool),
}

func (b *Broadcaster) Subscribe(pipelineID string) chan *core.Pipeline {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.clients[pipelineID] == nil {
		b.clients[pipelineID] = make(map[chan *core.Pipeline]bool)
	}

	ch := make(chan *core.Pipeline, 10)
	b.clients[pipelineID][ch] = true
	return ch
}

func (b *Broadcaster) Unsubscribe(pipelineID string, ch chan *core.Pipeline) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if clients, ok := b.clients[pipelineID]; ok {
		delete(clients, ch)
		close(ch)
		if len(clients) == 0 {
			delete(b.clients, pipelineID)
		}
	}
}

func (b *Broadcaster) Broadcast(p *core.Pipeline) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if clients, ok := b.clients[p.ID]; ok {
		for ch := range clients {
			select {
			case ch <- p:
			default:
			}
		}
	}
}
