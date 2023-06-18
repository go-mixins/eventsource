package main

import (
	"context"
	"log"

	"github.com/go-mixins/eventsource"
)

type Create struct {
	Ward int
	Name string
	Age  int
}

func (cp Create) Execute(ctx context.Context, p Patient) ([]eventsource.Event[Patient], error) {
	log.Printf("processing %T for %s", cp, eventsource.AggregateID(ctx))
	return []eventsource.Event[Patient]{PatientCreated(cp)}, nil
}

type Transfer struct {
	NewWard int
}

func (cp Transfer) Execute(_ context.Context, p Patient) ([]eventsource.Event[Patient], error) {
	if p.discharged {
		return nil, eventsource.ErrCommandAborted
	}
	return []eventsource.Event[Patient]{PatientTransferred(cp)}, nil
}

type Discharge struct{}

func (d Discharge) Execute(_ context.Context, p Patient) ([]eventsource.Event[Patient], error) {
	if p.discharged {
		return nil, nil
	}
	return []eventsource.Event[Patient]{PatientDischarged(d)}, nil
}
