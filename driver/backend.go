package driver

import (
	"context"
	"errors"
)

type Event[A any] struct {
	AggregateID      A
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

type Backend[A any] interface {
	Load(ctx context.Context, id A, fromVersion, toVersion int) ([]Event[A], error)
	Save(ctx context.Context, events []Event[A]) error
	Codec() Codec
}
