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

	// Instantiate the Event repository with all its dependencies
	repository := eventsource.NewRepository[Patient](backend, topic)

	// Service is dependent on Repository
	es := eventsource.NewService(repository)

	// Events must be registered once, before saving them into Repository
	es.Repository.RegisterEvents(PatientCreated{}, PatientTransferred{}, PatientDischarged{})

	// ProcessNotification method must be connected to pub/sub topic to receive events resulting from commands
	sub, err := pubsub.Handle(ctx, topicURL, es.ProcessNotification)
	if err != nil {
		panic(err)
	}
	defer sub.Shutdown(ctx)

	// Incoming events from the pub/sub bus are handled using Rules. Rules receive Aggregate and Event
	// as parameters and return list of Commands to execute next. All side-effects must occur in these Handlers,
	// outside the Command transactions. This allows Service to be idempotent and safe against concurrency conditions.
	if err := es.Handle(
		// This Rule transfer each created Patient to the new ward, as soon as the corresponding creation event
		// is arrived.
		//
		// Rules may send emails, update database records and generally call other services to produce side-effects.
		// We propose to return some Command from the Rule that registers the fact that side-effect was started.
		// This way, when the side-effect is completed, we can properly register its success or failure,
		// issuing Command to Service. Then another Rule will be fired, etc.
		eventsource.Rule(func(ctx context.Context, t Patient, e *PatientCreated) ([]eventsource.Command[Patient], error) {
			return []eventsource.Command[Patient]{
				Transfer{NewWard: t.ward + 1},
			}, nil
		}),
		// Just some arbitrary Rule to discharge Patient the moment it was transferred to another ward.
		// It essentially finishes Process for the Patient
		eventsource.Rule(func(ctx context.Context, t Patient, e *PatientTransferred) ([]eventsource.Command[Patient], error) {
			return []eventsource.Command[Patient]{
				Discharge{},
			}, nil
		}),
	); err != nil {
		panic(err)
	}
	// Creation of the Patient starts the Process determined by Rules above
	id := xid.New().String()
	if err := es.Execute(ctx, id, Create{Ward: 1, Name: "Vasya", Age: 21}); err != nil {
		slog.ErrorContext(ctx, "execution failed", "aggregate_id", id, "error", err)
		return
	}
	// Wait for process to complete
	time.Sleep(time.Second)
	// Simulate unhandled Command
	if err := es.Execute(ctx, id, Transfer{NewWard: 3}); errors.Is(err, eventsource.ErrCommandAborted) {
		slog.WarnContext(ctx, "not tranferring discharged patient", "aggregate_id", id)
	} else if err != nil {
		slog.ErrorContext(ctx, "execution failed", "aggregate_id", id, "error", err)
	}
	time.Sleep(time.Second)
}
