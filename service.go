package eventsource

import (
	"context"
	"errors"
	"time"

	"github.com/go-mixins/eventsource/driver"
)

// ErrTooManyRetries is returned when there are persistent concurrency errors detected
var ErrTooManyRetries = errors.New("too many retries")

type Service[T any] struct {
	Repository   *Repository[T]
	RetryTimeout time.Duration
	MaxRetries   int
}

func (s *Service[T]) retryTimeout() time.Duration {
	if s.RetryTimeout != 0 {
		return s.RetryTimeout
	}
	return time.Millisecond * 100
}

func (s *Service[T]) maxRetries() int {
	if s.MaxRetries != 0 {
		return s.MaxRetries
	}
	return 10
}

func (s *Service[T]) Execute(ctx context.Context, id string, cmd Command[T]) (rErr error) {
	for i := 0; i < s.maxRetries(); i++ {
		ag, err := s.Repository.Load(ctx, id)
		if err != nil {
			return err
		}
		if err := ag.Execute(cmd); err != nil {
			return err
		} else if err := s.Repository.Save(ctx, ag); errors.Is(err, driver.ErrConcurrency) {
			time.Sleep(s.retryTimeout())
			continue
		} else if err != nil {
			return err
		}
		return nil
	}
	return ErrTooManyRetries
}
