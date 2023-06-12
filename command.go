package eventsource

import "errors"

// ErrCommandAborted is returned from command's Execute method when it should be aborted
// without further modifications to the Aggregate
var ErrCommandAborted = errors.New("command aborted")

type Command[T any] interface {
	Execute(T) ([]Event[T], error)
}
