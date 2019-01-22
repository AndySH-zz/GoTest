package main

import (
// "encoding/csv"
// "gopkg.in/mgo.v2"
// "gopkg.in/mgo.v2/bson"
)

// const test string = "asdsd"

// func loadCountries() []country {
// 	return []country{}
// }

// func loadData(skillsFilePath string) error {
// 	file, err := os.Open(skillsFilePath)
// 	if err != nil {
// 		return err
// 	}

// 	defer file.Close()

// 	c := session.DB(dc.dbname).C(models.SkillsCollection)
// 	reader := csv.NewReader(file)
// 	for {
// 		record, err := reader.Read()
// 		if err == io.EOF {
// 			break
// 		} else if err != nil {
// 			return err
// 		}

// 		cqSupported := false
// 		if len(record) > 0 {
// 			num, _ := strconv.Atoi(record[1])
// 			cqSupported = num > 0
// 		}

// 		err = c.Insert(&models.Skill{Name: record[0], IsCQSupported: cqSupported})
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }
