package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/rs/xid"
	"gorm.io/driver/sqlite"

	"github.com/go-mixins/eventsource"
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
	backend := gorm.NewBackend[Patient](sqlite.Open("example.db"))
	if err := backend.Connect(true); err != nil {
		panic(err)
	}
	es := eventsource.Service[Patient]{
		Repository: &eventsource.Repository[Patient]{
			Backend: backend,
		},
	}
	ctx := context.TODO()
	es.Repository.RegisterEvents(PatientCreated{}, PatientTransferred{}, PatientDischarged{})
	es.Repository.Subscribe(func(n eventsource.Notification[Patient]) {
		log.Printf("signaled %T on %s: %+v", n.Event, n.AggregateID, n.Event)
	})
	id := xid.New().String()
	if err := es.Execute(ctx, id, Create{Ward: 1, Name: "Vasya", Age: 21}); err != nil {
		log.Fatal(err)
	}
	if err := es.Execute(ctx, id, Transfer{NewWard: 2}); err != nil {
		log.Fatal(err)
	}
	if err := es.Execute(ctx, id, Discharge{}); err != nil {
		log.Fatal(err)
	}
	if err := es.Execute(ctx, id, Transfer{NewWard: 3}); errors.Is(err, eventsource.ErrCommandAborted) {
		log.Print("not tranferring discharged patient")
	} else if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)
}
