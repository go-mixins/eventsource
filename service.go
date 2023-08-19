package eventsource

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/go-mixins/eventsource/driver"
)

// ErrTooManyRetries is returned when there are persistent concurrency errors detected
var ErrTooManyRetries = errors.New("too many retries")

type Service[T, A any] struct {
	Repository   *Repository[T, A] // Repository implementation
	RetryTimeout time.Duration     // Timeout to retry Commands on concurrency conflict. Defaults to 100ms.
	MaxRetries   int               // Maximum retries on concurrency conflict. After that error is signalled.
	ErrorHandler func(error)       // Specifies external handler for asynchronous errors.
	Logger       *slog.Logger

	handlers map[string]InternalRule[T]
	once     sync.Once
}

// NewService provides service with string IDs
func NewService[T any](r *Repository[T, string]) *Service[T, string] {
	return &Service[T, string]{
		Repository: r,
	}
}

func (s *Service[T, A]) retryTimeout() time.Duration {
	if s.RetryTimeout != 0 {
		return s.RetryTimeout
	}
	return time.Millisecond * 100
}

func (s *Service[T, A]) maxRetries() int {
	if s.MaxRetries != 0 {
		return s.MaxRetries
	}
	return 10
}

func (s *Service[T, A]) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

var ErrRetryCommand = errors.New("retry command")

// Execute the Command on Aggregate with specified ID and latest version available at the moment.
func (s *Service[T, A]) Execute(ctx context.Context, id A, cmd Command[T]) (rErr error) {
	var t T
	for i := 0; i < s.maxRetries(); i++ {
		ag, err := s.Repository.Load(ctx, id, -1)
		if err != nil {
			return err
		}
		if err := ag.Execute(ctx, cmd); errors.Is(err, ErrRetryCommand) {
			time.Sleep(s.retryTimeout())
			continue
		} else if err != nil {
			return fmt.Errorf("executing %T on %T with ID %v: %w", cmd, t, id, err)
		}
		if err := s.Repository.Save(ctx, ag); errors.Is(err, driver.ErrConcurrency) {
			time.Sleep(s.retryTimeout())
			continue
		} else if err != nil {
			return fmt.Errorf("saving aggregate %T with ID %v: %+v", t, id, err)
		}
		return nil
	}
	return fmt.Errorf("executing %T on %T with ID %v: %w", cmd, t, id, ErrTooManyRetries)
}
