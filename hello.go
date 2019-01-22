// You can edit this code!
// Click here and start typing.
package main

import (
	"fmt"
	"log"
	"time"

	"encoding/json"
	"github.com/nats-io/go-nats-streaming"
	"os"
	"os/signal"

	"github.com/ybbus/jsonrpc"
	cbmodels "gitlab.qarea.org/code-better/cb-backend/models"
	cqmodels "gitlab.qarea.org/jiraquality/codequality-backend/models"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func main() {
	// testTime()
	// addTarget()
	// getTargets()
	// getProblemsList()
	// getTypesList()
	// getIssuesTypesList()
	// getIssuesTypesList2()
	// initTestDB()
	// testMap()
	// convert()
	// getAutocomplete()

	// conn := getNATSConnection()
	// sub := NewStreamingClient(conn)
	// pub := NewStreamingClient(conn)
	// go startPublisher(pub)
	// go startSubscriber(sub)

	// startLoop()

	// adminLogin()
	// adminSettingsList()
	// testDuration()
	// testSort()
	// testAggregate()

	// convertToNewFormat()
	// calcCountries()
	// calcPlaces()

	// testConnectCommiters()
	testRequest()
}

func testAggregate() {
	searchString := "dev"

	type Result struct {
		InstanceToken  string `bson:"instance_token"`
		RepositoryLink string `bson:"repository_link"`
	}

	result := []Result{}

	d := newDatastorage("")
	session := d.session.Copy()
	defer session.Close()

	c := session.DB(d.name).C("collected_settings")

	condToken := testAggregateCreateCondition("$instance_token", searchString)
	condLink := testAggregateCreateCondition("$repository_link", searchString)

	queryPipeline := []bson.M{
		bson.M{"$project": bson.M{
			"_id":             0,
			"instance_token":  condToken,
			"repository_link": condLink,
		},
		},
	}

	pipe := c.Pipe(queryPipeline).AllowDiskUse()
	err := pipe.All(&result)
	if err != nil {
		log.Println("error", err)
	} else {
		log.Println("COOL", len(result))
	}
}

func testAggregateCreateCondition(key string, searchString string) bson.M {
	compare := bson.M{
		"$gte": []interface{}{
			bson.M{"$indexOfCP": []bson.M{
				bson.M{"$toLower": key},
				bson.M{"$toLower": searchString},
			}}, 0,
		}}

	cond := bson.M{"$cond": bson.M{
		"if":   compare,
		"then": key,
		"else": "$REMOVE",
	},
	}

	return cond
}

type Person struct {
	Name string
	Age  int
}

func testSort() {

}

func testDuration() {
	seconds := 10
	fmt.Print(time.Duration(seconds) * time.Second)
}

func getNATSConnection() stan.Conn {
	URL := fmt.Sprintf("nats://%s:%d", "localhost", 4222)

	sc, err := stan.Connect("test-cluster", "cb_server", stan.NatsURL(URL))
	if err != nil {
		log.Fatalf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err, URL)
	}

	log.Println("Connected to NATS server")
	return sc
}

func startLoop() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			log.Printf("captured %v, stopping and exiting..", sig)

			os.Exit(1)
		}
	}()

	select {}
}

type TestStreamData struct {
	Index int    `json:"index"`
	Data  string `json:"data"`
}

func startSubscriber(sub *Client) {
	msg := func(data []byte) {
		log.Println("Get message")
		var obj = TestStreamData{}
		err := json.Unmarshal(data, &obj)
		if err == nil {
			log.Println("Parse message", obj.Index, "data:", obj.Data)
		} else {
			log.Println("Error", err)
		}
	}

	err := sub.Subscribe(msg, "test")
	if err != nil {
		log.Println("Subscribe  error", err)
	} else {
		log.Println("Subscribed")
	}

}

func startPublisher(pub *Client) {
	for i := 0; i < 100; i++ {
		log.Println("Publish data")
		pub.Publish(TestStreamData{i, "hello"}, "test")
		time.Sleep(10 * time.Second)
	}
}

type Team struct {
	TeamID        bson.ObjectId   `json:"team_id"          bson:"_id,omitempty"`
	TeamTitle     string          `json:"team_title"       bson:"team_title"`
	InstanceToken *string         `json:"instance_token"   bson:"instance_token"`
	TeamMembers   []bson.ObjectId `json:"team_members"     bson:"team_members"`
}

type Issue struct {
	Title       string   `bson:"title"`
	IssueID     string   `bson:"issue_id"`
	IssueStatus string   `bson:"issue_status"`
	Titles      []string `bson:"titles"`
}

type Target struct {
	Title       string `bson:"title"`
	IssueID     string `bson:"issue_id"`
	IssueStatus string `bson:"issue_status"`
	PluginName  string `bson:"plugin_name"`
	RuleKey     string `bson:"rule_key"`
	Lang        string `bson:"lang"`
}

type IssuesList []Issue
type Teams []Team

type Group struct {
	Titles []string `bson:"titles"`
	Rules  []string `bson:"rules"`
}

type Groups []Group

var instanceToken = "4089768d1ba7e6a742d361db3a6a7a02"

func testMap() {
	set := make(map[string]bool)
	set["some string"] = true
	set["some string"] = false
	set["some stringw"] = true

	if len(set) == 2 {
		fmt.Print("COOL")
	} else {
		fmt.Print("WRONG")
	}
}

func testTime() {
	p := fmt.Println

	t := time.Now()
	p(t.Format(time.RFC3339))

	newT := t.Add(-(time.Minute * 15))
	p(newT.Format(time.RFC3339))
	//2017-12-14T00:00:00+02:00
	//2017-12-15T05:41:34+02:00

}

func initTestDB() {
	dataStorage := newDatastorage("")
	session := dataStorage.session.Copy()
	defer session.Close()

	const TargetsCollection = "collected_targets"

	session.DB(dataStorage.name).C(TargetsCollection).Create(&mgo.CollectionInfo{DisableIdIndex: false, ForceIdIndex: true})
	c := session.DB(dataStorage.name).C(TargetsCollection)
	c.EnsureIndex(mgo.Index{Key: []string{"instance_token"}, Unique: false, Background: false, Sparse: false})

	targets := []Target{
		{
			Title:       "Target 1",
			IssueID:     "IssueID1",
			IssueStatus: "OPEN",
			PluginName:  "js-common",
			RuleKey:     "Key1",
			Lang:        "Lang",
		},
		{
			Title:       "Target 2",
			IssueID:     "IssueID2",
			IssueStatus: "OPEN",
			PluginName:  "js-common",
			RuleKey:     "Key2",
			Lang:        "Lang",
		},
		{
			Title:       "the same rule",
			IssueID:     "IssueID3",
			IssueStatus: "CLOSED",
			PluginName:  "javascript",
			RuleKey:     "Key2",
			Lang:        "Lang",
		},
		{
			Title:       "the same rule and plugin name",
			IssueID:     "IssueID4",
			IssueStatus: "CLOSED",
			PluginName:  "javascript",
			RuleKey:     "Key2",
			Lang:        "Lang",
		},
		{
			Title:       "another lang",
			IssueID:     "IssueID3",
			IssueStatus: "CLOSED",
			PluginName:  "javascript",
			RuleKey:     "Key3",
			Lang:        "Lang2",
		},
	}

	for _, t := range targets {
		c := session.DB(dataStorage.name).C(TargetsCollection)
		err := c.Insert(t)
		if err != nil {
			fmt.Printf("Error: %s", err.Error())
		}
	}

	fmt.Printf("Finished")
}

func testRequest() {

}

func adminLogin() {
	rpcClient := newRPCClient()

	login := "user"
	pass := "password"
	args := &cqmodels.LoginRequest{
		Login:    &login,
		Password: &pass,
	}

	var reply *cqmodels.LoginResponse
	resp, err := rpcClient.Call("CQService.LoginUser", args)
	if err != nil {
		log.Fatal("dialing:", err)
	} else {
		if resp.Error != nil {
			log.Println(resp.Error.Message)
		} else {
			resp.GetObject(&reply)
			log.Println("Result", reply)
		}
	}
}

func adminSettingsList() {
	rpcClient := newRPCClient()

	token := "QhrSpzHxLBomLbLh"
	args := &cqmodels.IncomingArgs{
		Token: &token,
	}

	var reply *cqmodels.SettingsList
	resp, err := rpcClient.Call("CQService.GetAdminProjectSettingsList", args)
	if err != nil {
		log.Fatal("dialing:", err)
	} else {
		if resp.Error != nil {
			log.Println(resp.Error.Message)
		} else {
			resp.GetObject(&reply)
			log.Println("Result", reply)
		}
	}
}

func getAutocomplete() {
	/*
		{"id":"b70498a8-b5ba-4978-abcf-2b4218c70e3a","jsonrpc":"2.0","method":"CQService.GetDataAutocomplete",
			"params":{"instance_token":"cctestcloud.atlassian.net","project_id":10000,
				"repository_id":"7a57b4b40f757388d6d43e66f26e597b","search":"she","search_flag":["user"],
				"filter":{"scale":"repository","startDate":null,"endDate":null,"order":{"field":"commit_time","direction":"desc"},
				"pagination":{"page":0},"users":[],"languages":[]},"scale":"repository"}}



				{"id":"f20810ed-5ecf-4644-9ed9-1d301dd0ca72","jsonrpc":"2.0",
					"method":"CQService.GetDataAutocomplete",
					"params":{"instance_token":"ilighten.atlassian.net","project_id":10000,
						"repository_id":"7a57b4b40f757388d6d43e66f26e597b","search":"nikit",
						"search_flag":["user","commit","title","file"],
						"filter":{"scale":"instance","startDate":null,
							"endDate":null,"order":{"field":"commit_time","direction":"desc"},
							"pagination":{"page":0},"languages":[],"statuses":[],"severities":[],
							"users":[],"titles":[],"files":[],"commits":[]},"scale":"instance"}}
	*/
	instanceToken := "dev-add-on-1.atlassian.net"
	rpcClient := newRPCClient()

	projID := 10000
	scale := "repository"
	search := "ost"
	repositoryID := "7a57b4b40f757388d6d43e66f26e597b"
	searchFlag := []string{"user"}

	args := &cqmodels.ProjectDataListRequest{
		InstanceToken: &instanceToken,
		Scale:         &scale,
		ProjectID:     &projID,
		RepositoryID:  &repositoryID,
		Search:        &search,
		SearchFlag:    &searchFlag,
	}

	var reply *cqmodels.AutocompleteResponse
	resp, err := rpcClient.Call("CQService.GetDataAutocomplete", args)
	if err != nil {
		log.Fatal("dialing:", err)
	} else {
		if resp.Error != nil {
			log.Println(resp.Error.Message)
		} else {
			resp.GetObject(&reply)
			log.Println("Result", reply)
		}
	}
}

func getToday() time.Time {
	year, month, day := time.Now().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func getProblemsList() (*cqmodels.ScannedIssuesResponse, error) {

	instanceToken := "4089768d1ba7e6a742d361db3a6a7a02"
	rpcClient := newRPCClient()

	scale := "instance"
	hashes := []string{"165bd450442b2121736cd3168fd8116637bc1586"}

	filter := struct {
		StartDate  *time.Time `json:"start_date"`
		EndDate    *time.Time `json:"end_date"`
		Languages  *[]string  `json:"languages"`
		Severities *[]string  `json:"severities"`
		Statuses   *[]string  `json:"statuses"`
		Commits    *[]string  `json:"commits"`
		Titles     *[]string  `json:"titles"`
		Files      *[]string  `json:"files"`
		Types      *[]string  `json:"types"`
		Users      *[]struct {
			Name  *string `json:"name"`
			Email *string `json:"email"`
		} `json:"users"`
		Issues *[]struct {
			Name *string `json:"name"`
		} `json:"issues"`
		Order *struct {
			Field     *string `json:"field"`
			Direction *string `json:"direction"`
		} `json:"order"`
		Pagination *struct {
			Page *int `json:"page"`
		} `json:"pagination"`
	}{
		Commits:  &hashes,
		Statuses: &[]string{"OPEN"},
	}

	args := &cqmodels.ProjectDataListRequest{
		InstanceToken: &instanceToken,
		Scale:         &scale,
		Filter:        &filter,
	}

	var reply *cqmodels.ScannedIssuesResponse
	resp, err := rpcClient.Call("CQService.GetProblemsList", args)
	if err != nil {
		log.Fatal("dialing:", err)
	} else {
		if resp.Error != nil {
			log.Println(resp.Error.Message)
		} else {
			resp.GetObject(&reply)
			log.Println("Result", reply.OCount)

			for index, issue := range reply.Data {
				log.Println(index, " : ", issue.Title, "\n")
			}
		}
	}

	return reply, err
}

type IssueTypeGroup struct {
	Rule   string `bson:"rkey"`
	Plugin string `bson:"pname"`
	Title  string `bson:"title`
}

type IssueType struct {
	ID   IssueTypeGroup `bson:"_id"`
	Keys []string       `bson:"rules"`
}

type IssueTypes []IssueType

func getIssuesTypesList() {
	dataStorage := newDatastorage("")
	session := dataStorage.session.Copy()
	defer session.Close()

	issuesList := IssueTypes{}

	c := session.DB(dataStorage.name).C("collected_issues")
	queryPipeline := []bson.M{
		bson.M{
			"$match": bson.M{
				"issue_status":   "OPEN",
				"instance_token": "4089768d1ba7e6a742d361db3a6a7a02",
			},
		},
		bson.M{
			"$group": bson.M{
				"_id": bson.M{"rkey": "$rule_key", "pname": "$plugin_name", "title": "$title"},
				//"rules": bson.M{"$addToSet": "$rule_key"},
			},
		},
	}
	err := c.Pipe(queryPipeline).All(&issuesList)
	if err != nil {
		fmt.Printf("%s", err.Error())
	}

	fmt.Printf("%d", len(issuesList))
}

func getIssuesTypesList2() {
	dataStorage := newDatastorage("")
	session := dataStorage.session.Copy()
	defer session.Close()

	response := cqmodels.ScannedIssuesTypesResponse{}

	c := session.DB(dataStorage.name).C(cqmodels.IssuesCollection)
	queryPipeline := []bson.M{
		bson.M{"$match": bson.M{
			"issue_status":   "OPEN",
			"instance_token": "854b8eb15b3a99da0a3bbe7d1cd54af3",
		},
		},
		bson.M{
			"$group": bson.M{
				"_id":    "id",
				"vnames": bson.M{"$addToSet": "$issue_name"},
			},
		},
	}

	err := c.Pipe(queryPipeline).All(&response)
	if err != nil {
		fmt.Printf("%s", err.Error())
	}

	//fmt.Printf("%d", len(response.Data))
}

func getTypesList() {
	cbClient := newCBClient()

	resp, err := cbClient.Call("CBService.GetTargetTypesList", nil)
	if err != nil {
		log.Fatal("dialing:", err)
	} else {
		if resp.Error != nil {
			fmt.Printf("%s", resp.Error.Message)
		}

		var reply *cbmodels.GetTargetTypesListResponse
		resp.GetObject(&reply)

		fmt.Println("OK!")
	}
}

func getProblemsListOld() {
	// rpcClient := newRPCClient()

	// repId := "7a57b4b40f757388d6d43e66f26e597b";
	// projId := 22100
	// dt := "manager"
	// scale := "repository"

	// statuses := []string {"CLOSED"}

	// filter := struct {
	//     Statuses   *[]string   `json:"statuses"`
	// } {
	// 	Statuses: &statuses,
	// }

	// args := &cqmodels.ProjectDataListRequest{
	// 	InstanceToken: &instanceToken,
	// 	RepositoryID: &repId,
	// 	ProjectID: &projId,
	// 	Scale: &scale,
	// 	DashboardType: &dt,
	// 	Filter: &filter,
	// }

	// resp, err := rpcClient.Call("CQService.GetProblemsList", args)
	// if err != nil {
	// 	log.Fatal("dialing:", err)
	// } else {
	// 	if resp.Error != nil {
	// 		fmt.Printf("%s", resp.Error.Message)
	// 	}

	// 	var reply *cqmodels.ScannedIssuesResponse
	// 	resp.GetObject(&reply)
	// 	fmt.Printf("Result %d", reply.OCount)
	// }
}

func getTodayDate() time.Time {
	year, month, day := time.Now().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func getTargets() {
	// rpcClient := newCBClient()

	// args := &cbmodels.IncomingArgs{
	// 	InstanceToken: &instanceToken,
	// }

	// response, err := rpcClient.Call("CBService.GetTargetsList", args)
	// if err != nil {
	// 	log.Fatal("dialing:", err)
	// }

	// var reply *cbmodels.Targets
	// response.GetObject(&reply)

	// fmt.Printf("REPLY!: %d\n", len(*reply))

	// targetToUpdate := (*reply)[0]
	// targetToUpdate.Days = append(targetToUpdate.Days, cbmodels.TargetDay{
	// 	Day:      getTodayDate(),
	// 	Status:   cbmodels.TargetStatusNoMistakes,
	// 	Mistakes: 0,
	// })

	// updateTarget(targetToUpdate)
}

func addTarget() {
	// rpcClient := newCBClient()

	// args := &cbmodels.AddTargetArgs{
	// 	InstanceToken: &instanceToken,
	// 	Target: &cbmodels.Target{
	// 		Title:       "Add a new line at the end of this file.",
	// 		Description: "some description",
	// 		TotalDays:   21,
	// 	},
	// }

	// resp, err := rpcClient.Call("CBService.AddTarget", args)
	// if err != nil {
	// 	log.Fatal("dialing:", err)
	// } else {
	// 	if resp.Error != nil {
	// 		fmt.Printf("%s", resp.Error.Message)
	// 	} else {
	// 		fmt.Println("OK!")
	// 	}
	// }
}

func updateTarget(target cbmodels.Target) {
	rpcClient := newCBClient()

	args := &cbmodels.AddTargetArgs{
		InstanceToken: &instanceToken,
		Target:        &target,
	}

	resp, err := rpcClient.Call("CBService.AddTarget", args)
	if err != nil {
		log.Fatal("dialing:", err)
	} else {
		if resp.Error != nil {
			fmt.Printf("%s", resp.Error.Message)
		}

		fmt.Println("OK!")
	}
}

func removeTarget(id bson.ObjectId) {
	rpcClient := newCBClient()

	targetID := id
	args := &cbmodels.RemoveTargetArgs{
		InstanceToken: &instanceToken,
		ID:            &targetID,
	}

	resp, err := rpcClient.Call("CQService.RemoveTarget", args)
	if err != nil {
		log.Fatal("dialing:", err)
	} else {
		if resp.Error != nil {
			fmt.Printf("%s", resp.Error.Message)
		}

		fmt.Println("OK!")
	}
}

func getMembersListMongo() {
	dataStorage := newDatastorage("")
	committers, err := getCommitersList("dev-add-on-3.atlassian.net", dataStorage)
	if err != nil {
		fmt.Printf("getCommitersList error %s", err.Error())
	} else {
		fmt.Printf("getCommitersList count %d", len(committers))
	}
}

func getCommitersList(instanceToken string, d *database) (cqmodels.ProjectCommitters, error) {
	session := d.session.Copy()
	defer session.Close()

	membersList := cqmodels.ProjectCommitters{}
	c := session.DB(d.name).C(cqmodels.CommittersCollection)
	queryPipeline := bson.M{"instance_token": instanceToken}
	err := c.Find(queryPipeline).All(&membersList)
	if err != nil {
		return membersList, err
	}
	return membersList, nil
}

func newRPCClient() *jsonrpc.RPCClient {
	// return jsonrpc.NewRPCClient("https://sc-sindex.qarea.org/rpc")
	return jsonrpc.NewRPCClient("http://127.0.0.1:4327/rpc")
}

func newCBClient() *jsonrpc.RPCClient {
	return jsonrpc.NewRPCClient("http://127.0.0.1:4325/cb")
}

func getMembersListRPC() {
	fmt.Printf("1")
	rpcClient := newRPCClient()

	token := "dev-add-on-3.atlassian.net"
	args := &cqmodels.IncomingArgs{
		InstanceToken: &token,
	}

	fmt.Printf("2")
	response, err := rpcClient.Call("CQService.GetMembersList", args)
	if err != nil {
		fmt.Printf("3")
		fmt.Printf("dialing!: err")
		log.Fatal("dialing:", err)
		return
	}

	fmt.Printf("4")
	var reply *cqmodels.ProjectCommitters
	response.GetObject(&reply)

	fmt.Printf("5")
	fmt.Printf("REPLY!: %d\n", len(*reply))
}
