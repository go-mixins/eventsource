package eventsource

import (
	"context"
	"fmt"

	"github.com/go-mixins/eventsource/driver"
)

type Notification[T any] struct {
	AggregateID      string
	AggregateVersion int
	Event            Event[T]
}

type EventNotifier[T any] interface {
	Notify(...Notification[T]) error
}

type Codec[T any] interface {
	Unmarshal(eventType string, data []byte) (Event[T], error)
	Marshal(src Event[T]) ([]byte, error)
}

type Repository[T any] struct {
	Backend  driver.Backend
	Codec    Codec[T]
	Notifier EventNotifier[T]
}

func (es *Repository[T]) Load(ctx context.Context, id string) (*Aggregate[T], error) {
	evtDTOs, err := es.Backend.Load(ctx, id)
	if err != nil {
		return nil, err
	}
	evts := make([]Event[T], len(evtDTOs))
	for i, e := range evtDTOs {
		evt, err := es.Codec.Unmarshal(e.Type, e.Payload)
		if err != nil {
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

func (es *Repository[T]) Save(ctx context.Context, ag *Aggregate[T]) error {
	evts := ag.Events()
	evtDTOs := make([]driver.Event, len(evts))
	id, version := ag.ID(), ag.Version()
	for i, evt := range evts {
		data, err := es.Codec.Marshal(evt)
		if err != nil {
			return err
		}
		evtDTOs[i] = driver.Event{
			AggregateID:      id,
			AggregateVersion: version,
			Type:             fmt.Sprintf("%T", evt),
			Payload:          data,
		}
		version++
	}
	return es.Backend.Save(ctx, evtDTOs)
}
