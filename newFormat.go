package main

import (
	"gopkg.in/mgo.v2/bson"
	"log"
)

func convertToNewFormat() {
	initLogger()
	log.Println("Start script")

	dc := newDatastorage("branding_development")
	const collectionName = "collected_data_updated"
	const newCollectionName = "collected_data_updated_ready"
	const gitProfileCollectionName = "collected_gitprofile"

	session := dc.session.Copy()
	defer session.Close()

	queryPipeline := []bson.M{
		bson.M{
			"$limit": 100,
		},
		bson.M{
			"$unwind": "$processed_violations.commiters_list",
		},

		bson.M{
			"$unwind": "$processed_violations.commiters_list.collected_violations",
		},

		bson.M{
			"$addFields": bson.M{
				"processed_violations.commiters_list.total_effort": bson.M{"$add": []interface{}{
					"$processed_violations.commiters_list.collected_violations.codesmell_effort",
					"$processed_violations.commiters_list.collected_violations.bug_effort",
					"$processed_violations.commiters_list.collected_violations.vulnerability_effort",
					"$processed_violations.commiters_list.collected_violations.minor_effort",
					"$processed_violations.commiters_list.collected_violations.major_effort",
					"$processed_violations.commiters_list.collected_violations.critical_effort",
					"$processed_violations.commiters_list.collected_violations.blocker_effort",
					"$processed_violations.commiters_list.collected_violations.info_effort"},
				},
			},
		},

		bson.M{
			"$group": bson.M{
				"_id":             bson.M{"repository_id": "$repository_id", "stored_id": "$stored_id", "repository_lng": "$processed_violations.commiters_list.collected_violations.language"},
				"stored_id":       bson.M{"$first": "$stored_id"},
				"repository_id":   bson.M{"$first": "$repository_id"},
				"repository_lng":  bson.M{"$first": "$processed_violations.commiters_list.collected_violations.language"},
				"repository_type": bson.M{"$first": "$repository_type"},
				"processed_scan":  bson.M{"$first": "$processed_scan"},
				"created_at":      bson.M{"$first": "$created_at"},
				"processed_violations": bson.M{"$push": bson.M{
					"author":               "$processed_violations.commiters_list.author",
					"lines":                "$processed_violations.commiters_list.lines",
					"commits_count":        "$processed_violations.commiters_list.commits_count",
					"collected_violations": "$processed_violations.commiters_list.collected_violations",
					"total_effort":         "$processed_violations.commiters_list.total_effort"},
				},
				"total_effort":        bson.M{"$sum": "$processed_violations.commiters_list.total_effort"},
				"codesmell_count":     bson.M{"$sum": "$processed_violations.commiters_list.collected_violations.codesmell_count"},
				"bug_count":           bson.M{"$sum": "$processed_violations.commiters_list.collected_violations.bug_count"},
				"vulnerability_count": bson.M{"$sum": "$processed_violations.commiters_list.collected_violations.vulnerability_count"},
				"minor_count":         bson.M{"$sum": "$processed_violations.commiters_list.collected_violations.minor_count"},
				"critical_count":      bson.M{"$sum": "$processed_violations.commiters_list.collected_violations.critical_count"},
				"info_count":          bson.M{"$sum": "$processed_violations.commiters_list.collected_violations.info_count"},
				"major_count":         bson.M{"$sum": "$processed_violations.commiters_list.collected_violations.major_count"},
				"blocker_count":       bson.M{"$sum": "$processed_violations.commiters_list.collected_violations.blocker_count"},
			},
		},

		bson.M{
			"$addFields": bson.M{
				"td_ratio": bson.M{"$divide": []interface{}{"$total_effort", "$processed_scan.lines"}},
			},
		},

		bson.M{
			"$lookup": bson.M{
				"from":         gitProfileCollectionName,
				"localField":   "_id.stored_id",
				"foreignField": "stored_id",
				"as":           "profile",
			},
		},

		bson.M{
			"$unwind": "$profile",
		},

		bson.M{
			"$unwind": "$profile.stored_repositories_data",
		},

		bson.M{
			"$addFields": bson.M{
				"repository_link":       bson.M{"$cond": bson.M{"if": bson.M{"$eq": []interface{}{"$repository_id", "$profile.stored_repositories_data.repository_id"}}, "then": "$profile.stored_repositories_data.repository_link", "else": nil}},
				"repository_name":       bson.M{"$cond": bson.M{"if": bson.M{"$eq": []interface{}{"$repository_id", "$profile.stored_repositories_data.repository_id"}}, "then": "$profile.stored_repositories_data.repository_name", "else": nil}},
				"repository_size":       bson.M{"$cond": bson.M{"if": bson.M{"$eq": []interface{}{"$repository_id", "$profile.stored_repositories_data.repository_id"}}, "then": "$profile.stored_repositories_data.repository_size", "else": nil}},
				"repository_is_private": bson.M{"$cond": bson.M{"if": bson.M{"$eq": []interface{}{"$repository_id", "$profile.stored_repositories_data.repository_id"}}, "then": "$profile.stored_repositories_data.repository_is_private", "else": nil}},
			},
		},

		bson.M{
			"$match": bson.M{
				"repository_link": bson.M{"$exists": true, "$ne": nil},
			},
		},

		bson.M{
			"$group": bson.M{
				"_id": bson.M{"stored_id": "$_id.stored_id", "repository_lng": "$_id.repository_lng"},
				"repos": bson.M{"$push": bson.M{"repository_id": "$repository_id", "processing_time": "$processing_time",
					"repository_lng": "$repository_lng", "repository_type": "$repository_type",
					"processed_scan": "$processed_scan", "processed_violations": "$processed_violations",
					"repository_link": "$repository_link", "repository_name": "$repository_name",
					"repository_size": "$repository_size", "repository_is_private": "$repository_is_private",
					"total_violations": "$total_violations", "created_at": "$created_at", "td_ratio": "$td_ratio"}},
				"lines":               bson.M{"$sum": "$processed_scan.lines"},
				"codesmell_count":     bson.M{"$sum": "$codesmell_count"},
				"bug_count":           bson.M{"$sum": "$bug_count"},
				"vulnerability_count": bson.M{"$sum": "$vulnerability_count"},
				"minor_count":         bson.M{"$sum": "$minor_count"},
				"critical_count":      bson.M{"$sum": "$critical_count"},
				"info_count":          bson.M{"$sum": "$info_count"},
				"major_count":         bson.M{"$sum": "$major_count"},
				"total_effort":        bson.M{"$sum": "$total_effort"},
				"blocker_count":       bson.M{"$sum": "$blocker_count"},
				"profile":             bson.M{"$first": "$profile"},
			},
		},

		bson.M{
			"$addFields": bson.M{
				"total_violations.codesmell_count":     "$codesmell_count",
				"total_violations.bug_count":           "$bug_count",
				"total_violations.vulnerability_count": "$vulnerability_count",
				"total_violations.minor_count":         "$minor_count",
				"total_violations.critical_count":      "$critical_count",
				"total_violations.info_count":          "$info_count",
				"total_violations.major_count":         "$major_count",
				"total_violations.blocker_count":       "$blocker_count",
				"total_violations.total_effort":        "$total_effort",
			},
		},

		bson.M{
			"$project": bson.M{
				"_id":              1,
				"repos":            1,
				"lines":            1,
				"total_violations": 1,
				"login":            "$profile.stored_login",
				"avatar":           "$profile.stored_avatar_id",
				"name":             "$profile.stored_name",
				"email":            "$profile.stored_email",
				"location":         "$profile.stored_location",
			},
		},

		bson.M{
			"$addFields": bson.M{
				"td_ratio": bson.M{"$divide": []interface{}{"$total_violations.total_effort", "$lines"}},
				"td_loc": bson.M{"$multiply": []interface{}{100,
					bson.M{"$divide": []interface{}{
						"$total_violations.total_effort",
						bson.M{"$multiply": []interface{}{28.8, "$lines"}}}}}},
				// [TD/(LOC*28.8)]*100
			},
		},

		bson.M{"$out": newCollectionName},
	}

	c := session.DB(dc.name).C(collectionName)
	pipe := c.Pipe(queryPipeline).AllowDiskUse()

	type test struct {
	}
	dumm := []test{}
	err := pipe.All(&dumm)
	log.Println("Error: ", err)
}
