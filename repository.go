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

type Notification[T any] struct {
	AggregateID      string
	AggregateVersion int
	Event            Event[T]
}

type EventNotifier[T any] interface {
	Notify(...Notification[T]) error
}

type Codec interface {
	Unmarshal(data []byte, dest interface{}) error
	Marshal(src interface{}) ([]byte, error)
}

type Repository[T any] struct {
	Backend driver.Backend
	Codec   Codec

	notifier events.Event[Notification[T]]
	registry map[string]reflect.Type
}

func (r *Repository[T]) RegisterEvents(evts ...Event[T]) error {
	if r.registry == nil {
		r.registry = make(map[string]reflect.Type)
	}
	for _, e := range evts {
		t := reflect.TypeOf(e)
		if t.Kind() == reflect.Pointer {
			return fmt.Errorf("requred non-pointer type for Event[T]")
		}
		r.registry[fmt.Sprintf("%T", e)] = t
	}
	return nil
}

func (r *Repository[T]) Subscribe(h func(n Notification[T])) (unsub func()) {
	return r.notifier.Handle(h)
}

func (r *Repository[T]) instantiate(eventType string) (Event[T], bool) {
	t, ok := r.registry[eventType]
	if !ok {
		return nil, ok
	}
	res := reflect.New(t).Interface().(Event[T])
	return res, ok
}

func (r *Repository[T]) codec() Codec {
	if r.Codec != nil {
		return r.Codec
	}
	return driver.JSON{}
}

func (es *Repository[T]) Load(ctx context.Context, id string) (*Aggregate[T], error) {
	evtDTOs, err := es.Backend.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	evts := make([]Event[T], len(evtDTOs))
	for i, e := range evtDTOs {
		evt, ok := es.instantiate(e.Type)
		if !ok {
			return nil, fmt.Errorf("intantiating event %s: %w", e.Type, ErrUnknownEventType)
		}
		if err := es.codec().Unmarshal(e.Payload, &evt); err != nil {
			return nil, err
		}
		evts[i] = evt
	}
	res := &Aggregate[T]{
		id: id,
	}
	for _, e := range evts {
		res.On(e, false)
	}
	return res, nil
}

func (es *Repository[T]) Save(ctx context.Context, ag *Aggregate[T]) (rErr error) {
	var notifications []Notification[T]
	defer func() {
		if rErr != nil {
			return
		}
		for _, n := range notifications {
			es.notifier.Invoke(n)
		}
	}()
	evts := ag.Events()
	evtDTOs := make([]driver.Event, len(evts))
	id, version := ag.ID(), ag.Version()
	for i, evt := range evts {
		data, err := es.codec().Marshal(evt)
		if err != nil {
			return err
		}
		evtDTOs[i] = driver.Event{
			AggregateID:      id,
			AggregateVersion: version,
			Type:             fmt.Sprintf("%T", evt),
			Payload:          data,
		}
		notifications = append(notifications, Notification[T]{
			AggregateID:      id,
			AggregateVersion: version,
			Event:            evt,
		})
		version++
	}
	return es.Backend.Save(ctx, evtDTOs)
}
