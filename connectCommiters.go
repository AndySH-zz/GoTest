package main

import (
	"gopkg.in/mgo.v2/bson"
	"log"
	"time"
)

type GitHubID int64

type ProcessedScan struct {
	Lines                 int64   `json:"lines"                   bson:"lines"`
	Complexity            float64 `json:"complexity"              bson:"complexity"`
	DuplicatedLines       int     `json:"duplicated_lines"        bson:"duplicated_lines"`
	OwnBlocker            int     `json:"own_blocker"             bson:"own_blocker"`
	OwnCritical           int     `json:"own_critical"            bson:"own_critical"`
	OwnMinor              int     `json:"own_minor"               bson:"own_minor"`
	OwnMajor              int     `json:"own_major"               bson:"own_major"`
	BlockerViolations     int     `json:"blocker_violations"      bson:"blocker_violations"`
	CriticalViolations    int     `json:"critical_violations"     bson:"critical_violations"`
	MajorViolations       int     `json:"major_violations"        bson:"major_violations"`
	MinorViolations       int     `json:"minor_violations"        bson:"minor_violations"`
	SqualeIndex           int     `json:"squale_index"            bson:"squale_index"`
	SqualeRating          int     `json:"squale_rating"           bson:"squale_rating"`
	SqualeDebtRatio       float64 `json:"squale_debt_ratio"       bson:"squale_debt_ratio"`
	ReliabilityRating     int     `json:"reliability_rating"      bson:"reliability_rating"`
	SecurityRating        int     `json:"security_rating"         bson:"security_rating"`
	HightImpactViolations int     `json:"hight_impact_violations" bson:"hight_impact_violations"`
	LowImpactViolations   int     `json:"low_impact_violations"   bson:"low_impact_violations"`
	ViolationsRatio       float64 `json:"violations_ratio"        bson:"violations_ratio"`
}

type CollectedViolation struct {
	CodesmellCount      int    `json:"codesmell_count"      bson:"codesmell_count"`
	CodesmellEffort     int    `json:"codesmell_effort"     bson:"codesmell_effort"`
	BugCount            int    `json:"bug_count"            bson:"bug_count"`
	BugEffort           int    `json:"bug_effort"           bson:"bug_effort"`
	VulnerabilityCount  int    `json:"vulnerability_count"  bson:"vulnerability_count"`
	VulnerabilityEffort int    `json:"vulnerability_effort" bson:"vulnerability_effort"`
	MinorCount          int    `json:"minor_count"          bson:"minor_count"`
	MajorCount          int    `json:"major_count"          bson:"major_count"`
	CriticalCount       int    `json:"critical_count"       bson:"critical_count"`
	BlockerCount        int    `json:"blocker_count"        bson:"blocker_count"`
	InfoCount           int    `json:"info_count"           bson:"info_count"`
	MinorEffort         int    `json:"minor_effort"         bson:"minor_effort"`
	MajorEffort         int    `json:"major_effort"         bson:"major_effort"`
	CriticalEffort      int    `json:"critical_effort"      bson:"critical_effort"`
	BlockerEffort       int    `json:"blocker_effort"       bson:"blocker_effort"`
	InfoEffort          int    `json:"info_effort"          bson:"info_effort"`
	Language            string `json:"language"             bson:"language"`
}

func (cv CollectedViolation) TotalEffort() int64 {
	res := cv.BlockerEffort + cv.BugEffort + cv.CodesmellEffort + cv.CriticalEffort + cv.InfoEffort +
		cv.MajorEffort + cv.MinorEffort + cv.VulnerabilityEffort
	return int64(res)
}

type ViolationsInfo struct {
	Author             string             `json:"author"               bson:"author"`
	Lines              int64              `json:"lines"                bson:"lines"`
	CommitsCount       int                `json:"commits_count"        bson:"commits_count"`
	TotalEffort        int64              `json:"total_effort"         bson:"total_effort"`
	CollectedViolation CollectedViolation `json:"collected_violations" bson:"collected_violations"`
}

type TotalViolations struct {
	MinorCount         int   `json:"minor_count"          bson:"minor_count"`
	InfoCount          int   `json:"info_count"          bson:"info_count"`
	MajorCount         int   `json:"major_count"         bson:"major_count"`
	CodesmellCount     int   `json:"codesmell_count"     bson:"codesmell_count"`
	BugCount           int   `json:"bug_count"           bson:"bug_count"`
	VulnerabilityCount int   `json:"vulnerability_count" bson:"vulnerability_count"`
	CriticalCount      int   `json:"critical_count"      bson:"critical_count"`
	TotalEffort        int64 `json:"total_effort"        bson:"total_effort"`
}

func (tv *TotalViolations) add(v CollectedViolation) {
	tv.BugCount += v.BugCount
	tv.CodesmellCount += v.CodesmellCount
	tv.CriticalCount += v.CriticalCount
	tv.InfoCount += v.InfoCount
	tv.MajorCount += v.MajorCount
	tv.MinorCount += v.MinorCount
	tv.VulnerabilityCount += v.VulnerabilityCount

	tv.TotalEffort += v.TotalEffort()
}

func (tv *TotalViolations) subtract(v CollectedViolation) {
	tv.BugCount -= v.BugCount
	tv.CodesmellCount -= v.CodesmellCount
	tv.CriticalCount -= v.CriticalCount
	tv.InfoCount -= v.InfoCount
	tv.MajorCount -= v.MajorCount
	tv.MinorCount -= v.MinorCount
	tv.VulnerabilityCount -= v.VulnerabilityCount

	tv.TotalEffort -= v.TotalEffort()
}

type Repository struct {
	CreatedAt     time.Time        `json:"created_at"            bson:"created_at"`
	Type          string           `json:"repository_type"       bson:"repository_type"`
	Size          int              `json:"repository_size"       bson:"repository_size"`
	Private       bool             `json:"repository_is_private" bson:"repository_is_private"`
	Violations    []ViolationsInfo `json:"processed_violations"  bson:"processed_violations"`
	Link          string           `json:"repository_link"       bson:"repository_link"`
	Name          string           `json:"repository_name"       bson:"repository_name"`
	ID            GitHubID         `json:"repository_id"         bson:"repository_id"`
	Language      string           `json:"repository_lng"        bson:"repository_lng"`
	ProcessedScan ProcessedScan    `json:"processed_scan"        bson:"processed_scan"`
}

type DeveloperID struct {
	RecordID int    `json:"record_id"      bson:"stored_id"`
	Language string `json:"repository_lng" bson:"repository_lng"`
}

// Developer structure for developer Developer
type Developer struct {
	ID              DeveloperID     `json:"id"             bson:"_id"`
	Email           string          `json:"email"          bson:"email"`
	Location        string          `json:"location"       bson:"location"`
	Name            string          `json:"name"           bson:"name"`
	Login           string          `json:"login"          bson:"login"`
	Avatar          string          `json:"avatar"         bson:"avatar"`
	TD              float64         `json:"td"             bson:"td_ratio"`
	Loc             int64           `json:"loc"            bson:"lines"`
	Repos           []Repository    `json:"repos"          bson:"repos"`
	TotalViolations TotalViolations `json:"total_violations"      bson:"total_violations"`
	CountryPlace    int             `json:"country_place"          bson:"country_place"`
	GlobalPlace     int             `json:"global_place"          bson:"global_place"`
}

type DB struct {
	collecton string
	db        string
}

func testConnectCommiters() {
	connectCommiters(635912, []string{"peterstacho@gmail.com", "mchadaj@p29-16.labpk.inf.ug.edu.pl"})
}

func connectCommiters(storeID GitHubID, emails []string) {
	// get record by store id
	log.Println("Start script")

	dc := newDatastorage("branding_development")
	const collection = "collected_data_sonarlatest_copy"
	res, err := allRecordsByStoreID(storeID, collection, dc)

	if err != nil {
		log.Fatalln("Error: ", err)
	}

	log.Println("res: ", res)

	// get all records by email
	resEmail, err := allRecordsByEmail(emails, collection, dc)
	if err != nil {
		log.Fatalln("Error by email: ", err)
	}

	log.Println("res email: ", resEmail)

	arrToUpdate := []interface{}{}
	// check repos and recalc
	for _, d := range resEmail {
		for _, e := range emails {
			v, r, i := findCollectedViolation(&d, e)
			if v != nil && r != nil {
				added, updatedDev := addViolation(res, v, r)
				if added && updatedDev != nil {
					arrToUpdate = addDeveloperToUpdateArray(arrToUpdate, updatedDev)
					updatedDev = removeViolationFromRepo(&d, r, i)

					if updatedDev != nil {
						arrToUpdate = addDeveloperToUpdateArray(arrToUpdate, updatedDev)
					}
				}
			}
		}
	}

	// store all data
	updateDevelopers(arrToUpdate, collection, dc)
}

func addDeveloperToUpdateArray(arr []interface{}, dev *Developer) []interface{} {
	found := false
	for _, i := range arr {
		d := i.(*Developer)
		if d.ID.RecordID == dev.ID.RecordID && d.ID.Language == dev.ID.Language {
			found = true
			break
		}
	}

	if !found {
		arr = append(arr, dev)
	}

	return arr
}

func updateDevelopers(developers []interface{}, collection string, db *database) {
	session := db.session.Copy()
	defer session.Close()

	c := session.DB(db.name).C(collection)

	bulk := c.Bulk()
	bulk.Unordered()

	updatePairs := []interface{}{}
	for _, d := range developers {
		dev := *d.(*Developer)
		updatePairs = append(updatePairs, bson.M{"_id.stored_id": dev.ID.RecordID})
		updatePairs = append(updatePairs, bson.M{"$set": dev})
	}

	bulk.UpdateAll(updatePairs...)

	_, bulkErr := bulk.Run()
	if bulkErr != nil {
		log.Fatalln("update error: ", bulkErr)
	}
}

func removeViolationFromRepo(dev *Developer, r *Repository, violationIndex int) *Developer {
	v := r.Violations[violationIndex]

	r.Violations = append(r.Violations[:violationIndex], r.Violations[violationIndex+1:]...)

	dev.TotalViolations.subtract(v.CollectedViolation)
	dev.Loc -= v.Lines

	return dev
}

func addViolation(dev []Developer, v *ViolationsInfo, r *Repository) (bool, *Developer) {
	var devToAdd *Developer
	for _, d := range dev {
		if d.ID.Language == r.Language {
			devToAdd = &d
			break
		}
	}

	if devToAdd == nil {
		log.Fatalln("processViolation: can't find developer", dev, *v, *r)
	}

	added := false
	repo := findRepoInDeveloper(devToAdd, r)
	if repo == nil {
		devToAdd.Repos = append(devToAdd.Repos, *r)
		repo = &devToAdd.Repos[len(devToAdd.Repos)-1]
		repo.Violations = []ViolationsInfo{}
		added = true
	}

	if findViolationInRepo(v, repo) == -1 {
		repo.Violations = append(repo.Violations, *v)
		devToAdd.TotalViolations.add(v.CollectedViolation)
		devToAdd.Loc += v.Lines
		added = true
	}

	if !added {
		devToAdd = nil
	}

	return added, devToAdd
}

func findViolationInRepo(violation *ViolationsInfo, r *Repository) int {
	for i, v := range r.Violations {
		if v.Author == violation.Author {
			return i
		}
	}
	return -1
}

func totalViolations(violations []ViolationsInfo) (TotalViolations, int64) {
	tv := TotalViolations{}
	var lines int64
	for _, vi := range violations {
		lines += vi.Lines
		tv.add(vi.CollectedViolation)
	}

	return tv, lines
}

func findRepoInDeveloper(dev *Developer, repo *Repository) *Repository {
	for _, r := range dev.Repos {
		if r.ID == repo.ID {
			return &r
		}
	}

	return nil
}

func findCollectedViolation(dev *Developer, email string) (*ViolationsInfo, *Repository, int) {
	for repoID, r := range dev.Repos {
		for i, v := range r.Violations {
			if v.Author == email {
				repo := &dev.Repos[repoID]
				return &repo.Violations[i], repo, i
			}
		}
	}

	return nil, nil, -1
}

func allRecordsByStoreID(storeID GitHubID, collection string, db *database) ([]Developer, error) {
	session := db.session.Copy()
	defer session.Close()

	resp := []Developer{}

	queryPipeline := bson.M{
		"_id.stored_id": storeID,
	}

	c := session.DB(db.name).C(collection)
	err := c.Find(queryPipeline).All(&resp)
	return resp, err
}

func allRecordsByEmail(emails []string, collection string, db *database) ([]Developer, error) {
	session := db.session.Copy()
	defer session.Close()

	resp := []Developer{}

	orQuery := []bson.M{}
	for _, e := range emails {
		orQuery = append(orQuery, bson.M{"repos.processed_violations.author": e})
	}

	queryPipeline := bson.M{
		"$or": orQuery,
	}

	c := session.DB(db.name).C(collection)
	err := c.Find(queryPipeline).All(&resp)
	return resp, err
}
