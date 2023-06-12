package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/go-mixins/eventsource"
	"github.com/go-mixins/eventsource/driver"
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

type PatientCreated struct {
	ID   string
	Ward int
	Name string
	Age  int
}

func (pc PatientCreated) Apply(p *Patient) {
	p.id = pc.ID
	p.ward = pc.Ward
	p.name = pc.Name
	p.age = pc.Age
}

type PatientTransferred struct {
	NewWard int
}

func (pc PatientTransferred) Apply(p *Patient) {
	p.ward = pc.NewWard
}

type CreatePatient struct {
	ID   string
	Ward int
	Name string
	Age  int
}

func (cp CreatePatient) Execute(p Patient) ([]eventsource.Event[Patient], error) {
	return []eventsource.Event[Patient]{PatientCreated(cp)}, nil
}

type Transfer struct {
	NewWard int
}

func (cp Transfer) Execute(p Patient) ([]eventsource.Event[Patient], error) {
	if p.discharged {
		return nil, eventsource.ErrCommandAborted
	}
	return []eventsource.Event[Patient]{PatientTransferred(cp)}, nil
}

type Discharge struct{}

type PatientDischarged struct{}

func (PatientDischarged) Apply(p *Patient) {
	p.discharged = true
}

func (d Discharge) Execute(p Patient) ([]eventsource.Event[Patient], error) {
	if p.discharged {
		return nil, nil
	}
	return []eventsource.Event[Patient]{PatientDischarged(d)}, nil
}

func main() {
	es := eventsource.Service[Patient]{
		Repository: &eventsource.Repository[Patient]{
			Backend: &driver.InMemory{},
		},
	}
	ctx := context.TODO()
	es.Repository.RegisterEvents(PatientCreated{}, PatientTransferred{}, PatientDischarged{})
	es.Repository.Subscribe(func(n eventsource.Notification[Patient]) {
		log.Printf("signaled %T on %s: %+v", n.Event, n.AggregateID, n.Event)
	})
	if err := es.Execute(ctx, "1", CreatePatient{Ward: 1, Name: "Vasya", Age: 21}); err != nil {
		log.Fatal(err)
	}
	if err := es.Execute(ctx, "1", Transfer{NewWard: 2}); err != nil {
		log.Fatal(err)
	}
	if err := es.Execute(ctx, "1", Discharge{}); err != nil {
		log.Fatal(err)
	}
	if err := es.Execute(ctx, "1", Transfer{NewWard: 3}); errors.Is(err, eventsource.ErrCommandAborted) {
		log.Print("not tranferring discharged patient")
	} else if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)
}
