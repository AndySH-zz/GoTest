package main

import (
	"gopkg.in/mgo.v2"
	"log"
	"time"
)

type database struct {
	session *mgo.Session
	name    string
}

func newDatastorage(dbname string) *database {
	return &database{
		getMongoSession(),
		dbname,
	}
}

func getMongoSession() *mgo.Session {
	mongoDBDialInfo := &mgo.DialInfo{
		Addrs:    []string{"195.230.103.92"},
		Database: "admin",
		Username: "executor",
		Password: "^YHNmju7",
	}
	s, err := mgo.DialWithInfo(mongoDBDialInfo)
	if err != nil {
		log.Printf("MongoDB connection problems arise: %s, waiting 10 seconds for re-connection", err.Error())
		time.Sleep(10 * time.Second)
		return getMongoSession()
	}

	s.SetMode(mgo.Monotonic, true)
	return s
}
