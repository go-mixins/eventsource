package eventsource

type Aggregate[T any] struct {
	id      string
	data    T
	changes []Event[T]
	version int
}

func (a *Aggregate[T]) ID() string {
	return a.id
}

func (a *Aggregate[T]) On(e Event[T], isNew bool) {
	e.Apply(&a.data)
	if !isNew {
		a.version++
	}
}

func (a *Aggregate[T]) Execute(cmd Command[T]) error {
	evts, err := cmd.Execute(a.data)
	if err != nil {
		return err
	}
	for _, e := range evts {
		a.changes = append(a.changes, e)
		a.On(e, true)
	}
	return nil
}

func (a *Aggregate[T]) Events() []Event[T] {
	return a.changes
}

func (a *Aggregate[T]) Version() int {
	return a.version
}
