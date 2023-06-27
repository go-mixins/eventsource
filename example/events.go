package main

type PatientCreated struct {
	Ward int
	Name string
	Age  int
}

func (pc PatientCreated) Apply(p *Patient) {
	p.ward = pc.Ward
	p.name = pc.Name
	p.age = pc.Age
}

type PatientTransferred = Transfer

func (pc PatientTransferred) Apply(p *Patient) {
	p.ward = pc.NewWard
}

type PatientDischarged struct{}

func (PatientDischarged) Apply(p *Patient) {
	p.discharged = true
}
