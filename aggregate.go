package eventsource

import "context"

type Aggregate[T, A any] struct {
	id      A
	data    T
	changes []Event[T]
	version int
}

func (a *Aggregate[T, A]) ID() A {
	return a.id
}

func (a *Aggregate[T, A]) On(e Event[T], isNew bool) {
	e.Apply(&a.data)
	if !isNew {
		a.version++
	}
}

type aggregateIDKey struct{}

func (a *Aggregate[T, A]) context(ctx context.Context) context.Context {
	return context.WithValue(ctx, aggregateIDKey{}, a.id)
}

func AggregateID[A any](ctx context.Context) A {
	id, _ := ctx.Value(aggregateIDKey{}).(A)
	return id
}

func (a *Aggregate[T, A]) Execute(ctx context.Context, cmd Command[T]) error {
	evts, err := cmd.Execute(a.context(ctx), a.data)
	if err != nil {
		return err
	}
	for _, e := range evts {
		a.changes = append(a.changes, e)
		a.On(e, true)
	}
	return nil
}

func (a *Aggregate[T, A]) Events() []Event[T] {
	return a.changes
}

func (a *Aggregate[T, A]) Version() int {
	return a.version
}

func (a *Aggregate[T, A]) Data() T {
	return a.data
}
