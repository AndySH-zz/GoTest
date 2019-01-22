package main

import (
	"encoding/csv"
	"fmt"
	"gitlab.qarea.org/jiraquality/helpers-lib/logger"
	"gopkg.in/mgo.v2/bson"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

const (
	dbPath           = "/Users/andy/Downloads/GeoLite2-City-CSV_20180327/"
	countryCodeIndex = 4
	countryNameIndex = 5
	cityNameIndex    = 10
)

func separators() []string {
	return []string{" ", "-", ",", ".", "/", "\\"}
}

func calcCountries() {
	initLogger()
	// loadCountriesFromCsv()

	ds := newDatastorage("branding_development")
	const collectionName = "collected_data_updated_ready"

	locations, err := loadAllLocations(ds, collectionName)
	if err != nil {
		log.Println("loadCountries error", err)
		return
	}

	log.Println("Loaded", len(locations), "locations")
	locations = checkCountries(locations, ds, collectionName)

	log.Println("Can't find countries for", len(locations), "locations")
	// for _, v := range locations {
	// 	log.Println(v)
	// }

	locations = checkLocationsDBs(locations, ds, collectionName)
	log.Println("Result. Not found", len(locations), "locations:")
	for _, v := range locations {
		log.Println(v)
	}
}

func loadCountriesFromCsv() {
	files := locationsCsvFiles(dbPath)
	m := map[string]country{}
	for _, filePath := range files {
		records := loadCSV(filePath)
		for _, rec := range records {
			m[rec[countryNameIndex]] = country{Name: rec[countryNameIndex], Code: rec[countryCodeIndex]}
		}
	}

	log.Println("var countries = []country{")
	s := ""
	for _, value := range m {
		s = fmt.Sprintf("{Name: \"%s\", Code:\"%s\"},", value.Name, value.Code)
		log.Println(s)
	}
}

func locationsCsvFiles(path string) []string {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	res := []string{}
	for _, f := range files {
		if strings.Contains(f.Name(), "Locations") {
			filePath := fmt.Sprintf("%s%s", path, f.Name())
			res = append(res, filePath)
		}
	}

	return res
}

func checkLocationsDBs(locations []string, d *database, collection string) []string {
	files := locationsCsvFiles(dbPath)
	for _, filePath := range files {
		log.Println("Process file", filePath)
		records := loadCSV(filePath)
		log.Println("Locations count before file processing", len(locations))
		locations = checkLocationsDB(locations, records, d, collection)
		log.Println("Locations count after file processing", len(locations))
	}

	return locations
}

func initLogger() {
	logger.RedirectLog(&logger.Conf{
		Filename:   "log/convert.log",
		MaxSize:    0,
		MaxBackups: 0,
		MaxAge:     0,
	}, nil)
}

func loadCSV(filePath string) [][]string {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	return records
}

func checkLocationsDB(locations []string, records [][]string, d *database, collection string) []string {
	noCountryLocations := []string{}
	found := false
	counter := 0
	city, countryCode, countryName := "", "", ""
	for _, loc := range locations {
		for _, rec := range records {
			city = rec[cityNameIndex]

			if findByCity(city, loc) {
				countryCode = rec[countryCodeIndex]
				countryName = rec[countryNameIndex]
				log.Println("Found city", city, "in location", loc, "set country", countryName, "code", countryCode)
				updateLocation(loc, countryCode, d, collection)
				counter++
				found = true
				break
			}
		}

		if !found {
			noCountryLocations = append(noCountryLocations, loc)
		}

		found = false
	}

	log.Println("Found", counter, "locations by city")
	return noCountryLocations
}

func findByCity(city string, location string) bool {
	find := false
	separators := separators()
	for _, s := range separators {
		find = findByCityS(city, location, s)
		if find {
			break
		}
	}

	return find
}

func findByCityS(city string, location string, separator string) bool {
	val := ""
	find := false

	parts := strings.Split(location, separator)
	for _, p := range parts {
		val = strings.TrimSpace(p)
		if strings.EqualFold(val, city) {
			find = true
			break
		}
	}

	return find
}

func checkCountries(locations []string, d *database, collection string) []string {
	noCountryLocations := []string{}
	var code string
	counter := 0
	for _, v := range locations {
		code = findCountry(v)
		if code != "" {
			updateLocation(v, code, d, collection)
			counter++
		} else {
			noCountryLocations = append(noCountryLocations, v)
		}
	}

	log.Println("Found", counter, "countries")
	return noCountryLocations
}

func updateLocation(location string, countryCode string, dc *database, collection string) {
	session := dc.session.Copy()
	defer session.Close()

	c := session.DB(dc.name).C(collection)

	c.UpdateAll(bson.M{"location": location},
		bson.M{"$set": bson.M{"country_code": countryCode}})
}

func loadAllLocations(dc *database, collection string) ([]string, error) {
	session := dc.session.Copy()
	defer session.Close()

	type location struct {
		Locations []string `bson:"location"`
	}

	resp := []location{}

	queryPipeline := []bson.M{
		bson.M{
			"$project": bson.M{"_id": 0, "location": 1},
		},
		bson.M{
			"$group": bson.M{"_id": "_id", "location": bson.M{"$addToSet": "$location"}},
		},
	}

	c := session.DB(dc.name).C(collection)
	pipe := c.Pipe(queryPipeline).AllowDiskUse()
	err := pipe.All(&resp)

	result := []string{}
	if err == nil && len(resp) > 0 {
		for _, v := range resp[0].Locations {
			if v != "" {
				result = append(result, v)
			}
		}
	}

	return result, err
}
