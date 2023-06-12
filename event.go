package eventsource

type Event[T any] interface {
	Apply(*T)
}
