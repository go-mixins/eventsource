package eventsource

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/andviro/go-events"
	"github.com/go-mixins/eventsource/driver"
)

var ErrUnknownEventType = errors.New("unknown event type")

type Repository[T, A any] struct {
	Backend driver.Backend[A]

	notifier events.Event[Notification[T, A]]
	registry map[string]reflect.Type
}

// NewRepository creates Repository with most common case: string IDs
func NewRepository[T any](b driver.Backend[string]) *Repository[T, string] {
	return &Repository[T, string]{
		Backend: b,
	}
}

type Notification[T, A any] struct {
	AggregateID      A
	AggregateVersion int
	Event            Event[T]
}

type EventNotifier[T, A any] interface {
	Notify(...Notification[T, A]) error
}

func (r *Repository[T, A]) RegisterEvents(evts ...Event[T]) error {
	if r.registry == nil {
		r.registry = make(map[string]reflect.Type)
	}
	for _, e := range evts {
		t := reflect.TypeOf(e)
		if t.Kind() == reflect.Pointer {
			return fmt.Errorf("requred non-pointer type for Event[T]")
		}
		r.registry[t.Name()] = t
	}
	return nil
}

func (r *Repository[T, A]) Subscribe(h func(n Notification[T, A])) (unsub func()) {
	return r.notifier.Handle(h)
}

func (r *Repository[T, A]) instantiate(eventType string) (Event[T], bool) {
	t, ok := r.registry[eventType]
	if !ok {
		return nil, ok
	}
	res := reflect.New(t).Interface().(Event[T])
	return res, ok
}

func (es *Repository[T, A]) Load(ctx context.Context, id A) (*Aggregate[T, A], error) {
	evts, err := es.GetEvents(ctx, id, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("loading events for aggregate %v: %+v", id, err)
	}
	res := &Aggregate[T, A]{
		id: id,
	}
	for _, e := range evts {
		res.On(e, false)
	}
	return res, nil
}

func (es *Repository[T, A]) Save(ctx context.Context, ag *Aggregate[T, A]) (rErr error) {
	var notifications []Notification[T, A]
	defer func() {
		if rErr != nil {
			return
		}
		for _, n := range notifications {
			es.notifier.Invoke(n)
		}
	}()
	evts := ag.Events()
	evtDTOs := make([]driver.Event[A], len(evts))
	id, version := ag.ID(), ag.Version()

	for i, evt := range evts {
		data, err := es.Backend.Codec().Marshal(evt)
		if err != nil {
			return err
		}
		evtDTOs[i] = driver.Event[A]{
			AggregateID:      id,
			AggregateVersion: version,
			Type:             reflect.TypeOf(evt).Name(),
			Payload:          data,
		}
		notifications = append(notifications, Notification[T, A]{
			AggregateID:      id,
			AggregateVersion: version,
			Event:            evt,
		})
		version++
	}
	return es.Backend.Save(ctx, evtDTOs)
}

func (es *Repository[T, A]) GetEvents(ctx context.Context, id A, fromVersion, toVersion int) ([]Event[T], error) {
	evtDTOs, err := es.Backend.Load(ctx, id, fromVersion, toVersion)
	if err != nil {
		return nil, err
	}
	evts := make([]Event[T], len(evtDTOs))
	for i, e := range evtDTOs {
		evt, ok := es.instantiate(e.Type)
		if !ok {
			return nil, fmt.Errorf("intantiating event %s: %w", e.Type, ErrUnknownEventType)
		}
		if err := es.Backend.Codec().Unmarshal(e.Payload, &evt); err != nil {
			return nil, err
		}
		evts[i] = evt
	}
	return evts, nil
}
