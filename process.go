package eventsource

import (
	"context"
	"fmt"
	"reflect"
)

// InternalRule is an intenal representation of Rule function.
type InternalRule[T any] struct {
	f func(ctx context.Context, agg T, evt Event[T]) ([]Command[T], error)
	t reflect.Type
}

// Rule creates InternalRule from function that receives some Event and Aggregate data object
// and returns list of Commands that should be executed on that Event.
func Rule[T any, E Event[T]](f func(context.Context, T, E) ([]Command[T], error)) InternalRule[T] {
	var e E
	return InternalRule[T]{
		f: func(ctx context.Context, agg T, evt Event[T]) ([]Command[T], error) {
			return f(ctx, agg, evt.(E))
		},
		t: reflect.TypeOf(e),
	}
}

// Handle registers Rule for the Aggregate. The set of such rules
// manages Aggregate states based on Events similarly to finite state machine.
func (s *Service[T, A]) Handle(rules ...InternalRule[T]) error {
	s.once.Do(func() {
		s.Repository.notifier.Handle(s.processNotification)
	})
	for _, r := range rules {
		if r.t.Kind() == reflect.Pointer {
			return fmt.Errorf("requred non-pointer type for %s", r.t.Name())
		}
		if s.handlers == nil {
			s.handlers = make(map[string]InternalRule[T])
		}
		if _, ok := s.handlers[r.t.Name()]; ok {
			return fmt.Errorf("event for %s is already handled", r.t.Name())
		}
		s.handlers[r.t.Name()] = r
	}
	return nil
}

func (s *Service[T, A]) processNotification(evt Notification[T, A]) {
	ctx := context.TODO()
	ag, err := s.Repository.Load(ctx, evt.AggregateID, -1)
	if err != nil {
		s.signalError(fmt.Errorf("processing event %T: %w", evt.Payload, err))
		return
	}
	h, ok := s.handlers[evt.Type]
	if !ok {
		return
	}
	cmds, err := h.f(ctx, ag.data, evt.Payload)
	if err != nil {
		s.signalError(fmt.Errorf("handling event %T: %w", evt.Payload, err))
		return
	}
	for _, c := range cmds {
		if err := s.Execute(context.TODO(), evt.AggregateID, c); err != nil {
			s.signalError(fmt.Errorf("executing command %T for event %T: %w", c, evt.Payload, err))
			return
		}
	}
}

func (s *Service[T, A]) signalError(err error) {
	if s.ErrorHandler == nil {
		return
	}
	s.ErrorHandler(err)
}
