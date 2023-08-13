package eventsource

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/go-mixins/eventsource/driver"
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
	for i, r := range rules {
		if r.t.Kind() != reflect.Pointer {
			return fmt.Errorf("rule %d: need pointer to %s in parameter", i, r.t.Name())
		}
		if s.handlers == nil {
			s.handlers = make(map[string]InternalRule[T])
		}
		key := r.t.Elem().Name()
		if _, ok := s.handlers[key]; ok {
			return fmt.Errorf("event for %s is already handled", key)
		}
		s.handlers[key] = r
	}
	return nil
}

func (s *Service[T, A]) ProcessNotification(ctx context.Context, dto driver.Event[A]) (rErr error) {
	defer func() {
		attrs := []any{
			slog.String("event_type", fmt.Sprintf("%s", dto.Type)),
			slog.String("aggregate_id", fmt.Sprintf("%v", dto.AggregateID)),
			slog.String("aggregate_version", fmt.Sprintf("%d", dto.AggregateVersion)),
		}
		if rErr != nil {
			attrs = append(attrs, slog.String("err", rErr.Error()))
		}
		s.logger().With(attrs...).DebugContext(ctx, "processed event")
	}()
	ag, err := s.Repository.Load(ctx, dto.AggregateID, -1)
	if err != nil {
		return fmt.Errorf("loading aggregate: %w", err)
	}
	h, ok := s.handlers[dto.Type]
	if !ok {
		return nil
	}
	evt, err := s.Repository.toEvent(dto)
	if err != nil {
		return fmt.Errorf("receiving event %s: %+v", dto.Type, err)
	}
	cmds, err := h.f(ctx, ag.data, evt)
	if err != nil {
		return fmt.Errorf("handling event %s: %w", dto.Type, err)
	}
	for _, c := range cmds {
		if err := s.Execute(context.TODO(), dto.AggregateID, c); errors.Is(err, ErrCommandAborted) {
			s.logger().Debug("command skipped", "aggregateID", dto.AggregateID, "command", fmt.Sprintf("%T", c))
			continue
		} else if err != nil {
			return fmt.Errorf("executing command %T for event %s: %w", c, dto.Type, err)
		}
	}
	return nil
}

func (s *Service[T, A]) signalError(err error) {
	if s.ErrorHandler == nil {
		return
	}
	s.ErrorHandler(err)
}
