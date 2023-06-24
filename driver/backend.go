package driver

import (
	"context"
	"errors"
)

type Event struct {
	AggregateID      string
	AggregateVersion int
	Type             string
	Payload          []byte
}

// ErrConcurrency is returned on event version conflict when saving Aggregate
var ErrConcurrency = errors.New("concurrency triggered")

type Codec interface {
	Unmarshal(data []byte, dest interface{}) error
	Marshal(src interface{}) ([]byte, error)
}

type Backend interface {
	Load(ctx context.Context, id string) ([]Event, error)
	Save(ctx context.Context, events []Event) error
	Codec() Codec
}
