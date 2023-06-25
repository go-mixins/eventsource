package driver

import (
	"context"
	"sync"
)

type InMemory[A comparable] struct {
	store    map[A][]Event[A]
	evtsSeen map[A]map[int]struct{}
	mu       sync.RWMutex
}

func (m *InMemory[A]) Load(ctx context.Context, id A) ([]Event[A], error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.store[id], nil
}

func (m *InMemory[A]) Save(ctx context.Context, events []Event[A]) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.evtsSeen == nil {
		m.evtsSeen = make(map[A]map[int]struct{})
	}
	if m.store == nil {
		m.store = make(map[A][]Event[A])
	}
	for _, e := range events {
		evtsSeen := m.evtsSeen[e.AggregateID]
		if evtsSeen == nil {
			evtsSeen = make(map[int]struct{})
			for _, e := range m.store[e.AggregateID] {
				evtsSeen[e.AggregateVersion] = struct{}{}
			}
			m.evtsSeen[e.AggregateID] = evtsSeen
		}
		if _, ok := evtsSeen[e.AggregateVersion]; ok {
			return ErrConcurrency
		}
		evtsSeen[e.AggregateVersion] = struct{}{}
		m.store[e.AggregateID] = append(m.store[e.AggregateID], e)
	}
	return nil
}
