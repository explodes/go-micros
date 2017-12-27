package main

import (
	pb "github.com/explodes/go-micros/vessel-service/proto/vessel"
	"gopkg.in/mgo.v2"
)

const (
	dbName           = "shippy"
	vesselCollection = "vessels"
)

type Repository interface {
	FindAvailable(*pb.Specification) (*pb.Vessel, error)
	Create(*pb.Vessel) error
	Close()
}

type VesselRepository struct {
	session *mgo.Session
}

// Create a new vessel
func (repo *VesselRepository) Create(vessel *pb.Vessel) error {
	return repo.collection().Insert(vessel)
}

// FindAvailable finds an available vessel
func (repo *VesselRepository) FindAvailable(specification *pb.Specification) (*pb.Vessel, error) {
	var vessels []*pb.Vessel
	// Find normally takes a query, but as we want everything, we can nil this.
	// We then bind our vessels variable by passing it as an argument to .All().
	// That sets vessels to the result of the find query.
	// There's also a `One()` function for single results.
	err := repo.collection().Find(nil).All(&vessels)
	return vessels[0], err
}

// Close closes the database session after each query has ran.
// Mgo creates a 'master' session on start-up, it's then good practice
// to copy a new session for each request that's made. This means that
// each request has its own database session. This is safer and more efficient,
// as under the hood each session has its own database socket and error handling.
// Using one main database socket means requests having to wait for that session.
// I.e this approach avoids locking and allows for requests to be processed concurrently. Nice!
// But... it does mean we need to ensure each session is closed on completion. Otherwise
// you'll likely build up loads of dud connections and hit a connection limit. Not nice!
func (repo *VesselRepository) Close() {
	repo.session.Close()
}

func (repo *VesselRepository) collection() *mgo.Collection {
	return repo.session.DB(dbName).C(vesselCollection)
}
