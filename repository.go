package eventsource

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/go-mixins/pubsub"

	"github.com/go-mixins/eventsource/driver"
)

var ErrUnknownEventType = errors.New("unknown event type")

// Repository stores and retrieves events for Aggregates of type T.
// It also notifies subscribers on aggregate-related events.
type Repository[T, A any] struct {
	Backend  driver.Backend[A]
	EventBus pubsub.Topic[driver.Event[A]]

	registry map[string]reflect.Type
}

// NewRepository creates Repository with most common case: string IDs
func NewRepository[T any](b driver.Backend[string], topic pubsub.Topic[driver.Event[string]]) *Repository[T, string] {
	return &Repository[T, string]{
		Backend:  b,
		EventBus: topic,
	}
}

// RegisterEvents initializes marshaling/unmarshaling system.
// All events defined for the Aggregate must be registered at startup.
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

// instantiate event object for deserialization
func (r *Repository[T, A]) instantiate(eventType string) (Event[T], bool) {
	t, ok := r.registry[eventType]
	if !ok {
		return nil, ok
	}
	res := reflect.New(t).Interface().(Event[T])
	return res, ok
}

// Load Aggregate with specified ID from the Repository at certain version.
// If version is -1 then load its latest available version.
func (es *Repository[T, A]) Load(ctx context.Context, id A, version int) (*Aggregate[T, A], error) {
	evts, err := es.GetEvents(ctx, id, 0, version)
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

// Save specified Aggregate and its Events. Event subscribers are notified on successful save.
func (es *Repository[T, A]) Save(ctx context.Context, ag *Aggregate[T, A]) (rErr error) {
	evts := ag.Events()
	evtDTOs := make([]driver.Event[A], len(evts))
	id, version := ag.ID(), ag.Version()
	for i, evt := range evts {
		data, err := es.Backend.Codec().Marshal(evt)
		if err != nil {
			return err
		}
		t := reflect.TypeOf(evt).Name()
		evtDTOs[i] = driver.Event[A]{
			AggregateID:      id,
			AggregateVersion: version,
			Type:             t,
			Payload:          data,
		}
		version++
	}
	return es.Backend.Save(ctx, evtDTOs, func() error {
		for _, n := range evtDTOs {
			if err := es.EventBus.Send(ctx, n); err != nil {
				return err
			}
		}
		return nil
	})
}

func (es *Repository[T, A]) toEvent(e driver.Event[A]) (Event[T], error) {
	evt, ok := es.instantiate(e.Type)
	if !ok {
		return nil, fmt.Errorf("intantiating event %s: %w", e.Type, ErrUnknownEventType)
	}
	if err := es.Backend.Codec().Unmarshal(e.Payload, &evt); err != nil {
		return nil, fmt.Errorf("unmarshaling %s: %+v", e.Type, err)
	}
	return evt, nil
}

// GetEvents returns event range [fromVersion, toVersion] for Aggregate with specified ID.
// If toVersion is -1, all available events with version greater or equal to fromVersion are returned.
func (es *Repository[T, A]) GetEvents(ctx context.Context, id A, fromVersion, toVersion int) ([]Event[T], error) {
	evtDTOs, err := es.Backend.Load(ctx, id, fromVersion, toVersion)
	if err != nil {
		return nil, err
	}
	evts := make([]Event[T], len(evtDTOs))
	for i, e := range evtDTOs {
		evt, err := es.toEvent(e)
		if err != nil {
			return nil, fmt.Errorf("converting DTO to real event: %w", err)
		}
		evts[i] = evt
	}
	return evts, nil
}
