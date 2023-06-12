package main

import "github.com/go-mixins/eventsource"

type Create struct {
	ID   string
	Ward int
	Name string
	Age  int
}

func (cp Create) Execute(p Patient) ([]eventsource.Event[Patient], error) {
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

func (d Discharge) Execute(p Patient) ([]eventsource.Event[Patient], error) {
	if p.discharged {
		return nil, nil
	}
	return []eventsource.Event[Patient]{PatientDischarged(d)}, nil
}
