package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"gorm.io/driver/sqlite"

	"github.com/rs/xid"

	g "github.com/go-mixins/gorm/v4"
	"github.com/go-mixins/log/v2"
	"github.com/go-mixins/pubsub"
	_ "github.com/go-mixins/pubsub/gocloud"
	_ "gocloud.dev/pubsub/mempubsub"

	"github.com/carlmjohnson/versioninfo"

	"github.com/go-mixins/eventsource"
	"github.com/go-mixins/eventsource/driver"
	"github.com/go-mixins/eventsource/driver/gorm"
)

type Patient struct {
	id         string
	ward       int
	name       string
	age        int
	discharged bool
}

func (p Patient) ID() string {
	return p.id
}

func main() {
	logger := log.Wrap(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	}))
	ctx := log.WithAttrs(context.TODO(), slog.String("version", versioninfo.Revision))
	slog.SetDefault(slog.New(logger))
	gormBackend := &g.Backend{Driver: sqlite.Open("example.db")}
	if err := gormBackend.Connect(); err != nil {
		slog.ErrorContext(ctx, "failed to connect DB", "error", err)
	}
	backend := gorm.NewBackend[Patient, string](gormBackend)
	if err := backend.Connect(true); err != nil {
		panic(err)
	}
	topicURL := "gocloud+json+mem://patient-events"
	topic, err := pubsub.OpenTopic[driver.Event[string]](ctx, topicURL)
	if err != nil {
		panic(err)
	}
	defer topic.Shutdown(ctx)
	repository := eventsource.NewRepository[Patient](backend, topic)
	es := eventsource.NewService(repository)
	es.Repository.RegisterEvents(PatientCreated{}, PatientTransferred{}, PatientDischarged{})
	sub, err := pubsub.Handle(ctx, topicURL, es.ProcessNotification)
	if err != nil {
		panic(err)
	}
	defer sub.Shutdown(ctx)
	if err := es.Handle(
		eventsource.Rule(func(ctx context.Context, t Patient, e *PatientCreated) ([]eventsource.Command[Patient], error) {
			return []eventsource.Command[Patient]{
				Transfer{NewWard: t.ward + 1},
			}, nil
		}),
		eventsource.Rule(func(ctx context.Context, t Patient, e *PatientTransferred) ([]eventsource.Command[Patient], error) {
			return []eventsource.Command[Patient]{
				Discharge{},
			}, nil
		}),
	); err != nil {
		panic(err)
	}
	id := xid.New().String()
	if err := es.Execute(ctx, id, Create{Ward: 1, Name: "Vasya", Age: 21}); err != nil {
		slog.ErrorContext(ctx, "execution failed", "aggregate_id", id, "error", err)
		return
	}
	time.Sleep(time.Second) // Wait for process to complete
	if err := es.Execute(ctx, id, Transfer{NewWard: 3}); errors.Is(err, eventsource.ErrCommandAborted) {
		slog.WarnContext(ctx, "not tranferring discharged patient", "aggregate_id", id)
	} else if err != nil {
		slog.ErrorContext(ctx, "execution failed", "aggregate_id", id, "error", err)
	}
	time.Sleep(time.Second)
}
