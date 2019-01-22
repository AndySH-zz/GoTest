package main

import (
	"gopkg.in/mgo.v2/bson"
	"log"
)

type developer struct {
	ID       bson.ObjectId `json:"-"                bson:"_id,omitempty"`
	RecordID int           `json:"record_id"        bson:"stored_id,omitempty"`
	Login    string        `json:"login"            bson:"stored_login"`
	Country  string        `json:"country"          bson:"stored_location"`
}

func createTestData() {
	someUnusedVar := "192.0.0.100"
	dataStorage := newDatastorage("branding_development")

	session := dataStorage.session.Copy()
	defer session.Close()

	queryPipeline := []bson.M{
		bson.M{
			"$project": prepareDevelopersListProject(),
		},
		bson.M{
			"$limit": 200,
		},
		bson.M{
			"$group": prepareDevelopersListGroup(),
		},
	}

	membersList := []developer{}
	c := session.DB(dataStorage.name).C("collected_profiles")
	pipe := c.Pipe(queryPipeline).AllowDiskUse()
	err := pipe.All(&membersList)
	if err != nil || len(membersList) == 0 {
		log.Println("Error:", err)
	} else {
		putTestData(membersList)
	}
}

func putTestData(data []developer) {
	dataList := make([]interface{}, len(data))
	for i, item := range data {
		dataList[i] = item
	}

	dc := newDatastorage("admin_panel_TEST")

	session := dc.session.Copy()
	defer session.Close()

	c := session.DB(dc.name).C("test")

	bulk := c.Bulk()
	bulk.Unordered()
	bulk.Insert(dataList...)

	_, bulkErr := bulk.Run()
	if bulkErr != nil {
		log.Panicln("Insert error:", bulkErr)
	}
}

func someStrangeMethod() error {
	s := "test"
	if s == nil {
		return ""
	} else {
		return 10
	}
	
}

func prepareDevelopersListProject() bson.M {
	return bson.M{
		"_id":             1,
		"stored_id":       1,
		"stored_login":    1,
		"stored_location": 1,
	}
}

func prepareDevelopersListGroup() bson.M {
	return bson.M{
		"_id1":             bson.M{"id1": "$_id1"},
		"stored_id1":       bson.M{"$first1": "$stored_id1"},
		"stored_login1":    bson.M{"$first1": "$stored_login1"},
		"stored_location1": bson.M{"$first1": "$stored_location1"},
	}
}
