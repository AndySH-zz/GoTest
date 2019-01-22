package main

import (
	"gopkg.in/mgo.v2/bson"
	"log"
)

func calcPlaces() {
	log.Println("Start script")

	dc := newDatastorage("branding_development")
	const collectionName = "collected_data_sonarlatest_copy"
	const newCollectionName = "result"

	ranges := getLinesRangeList()
	langs := getLanguagesList()
	for _, l := range langs {
		for _, r := range ranges {
			updateRangeAndLang(dc, collectionName, r, l)
		}
	}

	getAll(dc, collectionName, ranges[1], langs[0])

	log.Println("finish script")
}

const maxCount = 20

func updateRangeAndLang(dc *database, collection string, rng LinesRange, lang string) error {
	total, err := calcDevelopers(dc, collection, rng, lang)
	if err != nil {
		log.Fatalln("Can't calc developers", err)
	}

	updated := 0
	for updated < total {
		limit := maxCount
		if limit > (total - updated) {
			limit = total - updated
		}

		err := updateRangeAndLangChunk(dc, collection, rng, lang, updated, limit)
		if err != nil {
			return err
		}
		updated += limit
	}

	log.Println("Range", rng, " lang", lang, " count", total)
	return nil
}

func updateRangeAndLangChunk(dc *database, collection string, rng LinesRange, lang string, skip, limit int) error {
	developers, err := getDevelopersListForProfile(dc, collection, rng, lang, skip, limit)
	log.Println("Params:", rng, lang, "skip: ", skip, "limit", limit)
	if err != nil {
		log.Println("Error", err)
	} else {
		log.Println("Found ", len(developers), "records")
	}

	session := dc.session.Copy()
	defer session.Close()

	c := session.DB(dc.name).C(collection)

	bulk := c.Bulk()
	bulk.Unordered()

	place := skip + 1
	updatePairs := []interface{}{}
	for _, d := range developers {
		d.GlobalPlace = place
		place++

		updatePairs = append(updatePairs, bson.M{"_id.stored_id": d.ID.RecordID})
		updatePairs = append(updatePairs, bson.M{"$set": d})
	}

	bulk.UpdateAll(updatePairs...)
	_, bulkErr := bulk.Run()
	if bulkErr != nil {
		log.Fatalln("update error: ", bulkErr)
	}

	return err
}

func getAll(dc *database, collection string, rng LinesRange, lang string) {
	session := dc.session.Copy()
	defer session.Close()

	queryPipeline := []bson.M{
		bson.M{
			"$match": prepareDevelopersListFilter(rng, lang),
		},
		prepareDevelopersSorting(),
		bson.M{
			"$out": collection + lang,
		},
	}

	c := session.DB(dc.name).C(collection)
	pipe := c.Pipe(queryPipeline).AllowDiskUse()

	type test struct {
	}
	dumm := []test{}
	err := pipe.All(&dumm)
	log.Println("Error: ", err)
}

func getDevelopersListForProfile(dc *database, collection string, rng LinesRange, lang string, skip, limit int) ([]Developer, error) {

	session := dc.session.Copy()
	defer session.Close()

	resp := []Developer{}

	queryPipeline := []bson.M{
		bson.M{
			"$project": prepareDevelopersListForProfileProject(),
		},
		bson.M{
			"$match": prepareDevelopersListFilter(rng, lang),
		},
		prepareDevelopersSorting(),
		bson.M{
			"$skip": skip,
		},
		bson.M{
			"$limit": limit,
		},
	}

	c := session.DB(dc.name).C(collection)
	pipe := c.Pipe(queryPipeline).AllowDiskUse()
	err := pipe.All(&resp)
	return resp, err
}

// LinesRange represents country type
type LinesRange struct {
	Min int64 `json:"min"`
	Max int64 `json:"max"`
}

func prepareDevelopersSorting() bson.M {
	order := bson.M{"$sort": bson.D{{Name: "td_ratio", Value: 1},
		{Name: "lines", Value: -1}},
	}

	return order
}

func prepareDevelopersListForProfileProject() bson.M {
	return bson.M{
		"_id":      1,
		"lines":    1,
		"td_ratio": 1,
	}
}

func getLinesRangeList() []LinesRange {
	return []LinesRange{
		{Min: 1, Max: 100}, {Min: 100, Max: 1000}, {Min: 1000, Max: 5000},
		{Min: 5000, Max: 10000}, {Min: 10000, Max: 20000}, {Min: 20000, Max: 50000},
		{Min: 50000},
	}
}

func getLanguagesList() []string {
	return []string{"JavaScript", "PHP"}
}

func calcDevelopers(dc *database, collection string, rng LinesRange, lang string) (int, error) {
	session := dc.session.Copy()
	defer session.Close()

	filterPipeline := prepareDevelopersListFilter(rng, lang)

	countQueryPipeline := []bson.M{
		bson.M{
			"$project": bson.M{
				"lines": 1,
			},
		},
		bson.M{
			"$match": filterPipeline,
		},
		bson.M{
			"$count": "count",
		},
	}

	type Count struct {
		Count int `bson:"count"`
	}

	countRes := []Count{}
	c := session.DB(dc.name).C(collection)
	pipe := c.Pipe(countQueryPipeline).AllowDiskUse()
	err := pipe.All(&countRes)
	if err != nil || len(countRes) == 0 {
		return 0, err
	}

	return countRes[0].Count, nil
}

func prepareDevelopersListFilter(rng LinesRange, lang string) bson.M {
	filterPipeline := bson.M{}

	gteFilter := bson.M{}
	if rng.Min > 0 {
		gteFilter = bson.M{"lines": bson.M{"$gte": rng.Min}}
	}

	ltFilter := bson.M{}
	if rng.Max > 0 {
		ltFilter = bson.M{"lines": bson.M{"$lt": rng.Max}}
	}

	// country := ""
	// if filter.Country != nil && filter.Country.Code != "" && filter.Country.Name != "GLOBAL" {
	// 	country = filter.Country.Code
	// }

	filterPipeline = bson.M{
		"$and": []interface{}{
			bson.M{"lines": bson.M{"$gt": 0}},
			prepareFieldFilter(&lang, "_id.repository_lng"),
			// prepareFieldFilter(&country, "country_code"),
			gteFilter,
			ltFilter,
		},
	}

	return filterPipeline
}

func prepareFieldFilter(filterData *string, fieldName string) bson.M {
	res := bson.M{}
	if filterData != nil && *filterData != "" {
		res = bson.M{fieldName: *filterData}
	}

	return res
}
