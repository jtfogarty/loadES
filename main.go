package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/olivere/elastic"
)

type Verse struct {
	BCV        string `json:"bcv"`
	KJVText    string `json:"kvj_text"`
	ASVText    string `json:"asv_text"`
	DRText     string `json:"dr_text"`
	DarbText   string `json:"darb_text"`
	EngRText   string `json:"engr_text"`
	WebText    string `json:"web_text"`
	WldEngText string `json:"wld_eng_text"`
	YngLitText string `json:"young_lit_text"`
	AKJVText   string `json:"amer_kjv_text"`
	WeyText    string `json:"wey_text"`
}

const (
	indexName = "bible-v1"
	docType   = "_doc"
)

type BCV struct {
	Book    string `json: "book"`
	Chapter int    `json:"chapter"`
	Verse   int    `json: "verse"`
}

type VerseDoc struct {
	EpochTime int64     `json: "epochtime_loaded"`
	Version   Version   `json: "version"`
	Book      string    `json: "book"`
	ChapterNo int       `json: "chapter_no"`
	VerseNo   int       `json; "verse_no"`
	Verse     string    `json: "verse"`
	ESid      uuid.UUID `json: "es_id"`
	UTCTime   time.Time `json: "utc"`
}

type Version struct {
	ShortName string `json: "short_name"`
	LongName  string `json: "long_name"`
}

func checkErr(e error) {
	if e != nil {
		panic(e)
	}
}

var (
	client *elastic.Client
)

func getEnvDeleteIndex() bool {
	if strings.ToLower(os.Getenv("DELETE_INDEX")) == "true" {
		return true
	} else {
		return false
	}

}

func init() {
	var err error
	client, err = elastic.NewClient(elastic.SetURL("http://localhost:9200"))
	checkErr(err)
	ctx := context.Background()
	if getEnvDeleteIndex() {
		deleteIndex, err := client.DeleteIndex(indexName).Do(ctx)
		fmt.Println("Index deleted ", deleteIndex.Acknowledged)
		checkErr(err)
	}
}
func main() {
	createIndex()
	readJSON2ES()
}
func readJSON2ES() {
	ctx := context.Background()
	jsonFile, err := os.Open("/Users/jeff/go/src/github.com/jtfogarty/createJSONBible/os-bibles-escape-chr.json")
	checkErr(err)
	//byteValue, err := ioutil.ReadAll(jsonFile)
	checkErr(err)
	jsonDecoder := json.NewDecoder(jsonFile)
	bulk := client.Bulk().Index(indexName).Type(docType)
	for {
		var verses []Verse
		if err := jsonDecoder.Decode(&verses); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal("Decode: ", err)
		}
		for _, verse := range verses {
			vd := createDocStruc(verse.BCV, verse.KJVText, "kjv")
			bulk.Add(elastic.NewBulkIndexRequest().Id(vd.ESid.String()).Doc(vd))
			/*createDocStruc(verse.BCV, verse.ASVText, "asv")
			createDocStruc(verse.BCV, verse.DRText, "dr")
			createDocStruc(verse.BCV, verse.DarbText, "darb")
			createDocStruc(verse.BCV, verse.EngRText, "EngRe")
			createDocStruc(verse.BCV, verse.WebText, "web")
			createDocStruc(verse.BCV, verse.WldEngText, "wldEng")
			createDocStruc(verse.BCV, verse.YngLitText, "Young")
			createDocStruc(verse.BCV, verse.AKJVText, "akj")
			createDocStruc(verse.BCV, verse.WeyText, "wey")*/
		}
		//		fmt.Println(json.Marshal(verse))
	}
	if _, err := bulk.Do(ctx); err != nil {
		checkErr(err)
	}
	checkErr(err)
	fmt.Println("Data Loaded")
}

var VersionName = map[string]string{
	"kjv":    "King James Bible",
	"asv":    "American Standard Version",
	"dr":     "Douay-Rheims Bible",
	"darb":   "Darby Bible Translation",
	"EngRe":  "English Revised Version",
	"web":    "Webster Bible Translation",
	"wldEng": "World English Bible",
	"Young":  "Young's Literal Translation",
	"akj":    "American King James Version",
	"wey":    "Weymouth New Testament",
}

func createDocStruc(bcv string, text string, version string) *VerseDoc {
	var vd VerseDoc
	var re = regexp.MustCompile(`[0-9]+:[0-9]+`)
	var cv string

	//fmt.Printf("Book Chapter Verse %s, Text %s, Version %s", bcv, text, version)
	vd.EpochTime = time.Now().UnixNano()
	vd.Version.LongName = VersionName[version]
	vd.Version.ShortName = version
	cv = re.FindString(bcv)
	cvA := strings.Split(cv, ":")
	vd.Book = strings.TrimSpace(bcv[:len(bcv)-len(cv)])
	vd.ChapterNo, _ = strconv.Atoi(cvA[0])
	vd.VerseNo, _ = strconv.Atoi(cvA[1])
	vd.Verse = strings.TrimSpace(text)
	vd.ESid = uuid.Must(uuid.NewRandom())
	vd.UTCTime = time.Now().UTC()
	return &vd
	//b, err := json.Marshal(vd)
	//checkErr(err)
	//_, err := client.Index().Index(indexName).Type(docType).BodyJson(string(b)).Do(ctx)
	//fmt.Println(index.Acknowledged)
	//checkErr(err)
	//fmt.Println(string(b))
}

func createIndex() {
	ctx := context.Background()
	mapping := `{
		"settings":{
			"number_of_shards":1,
			"number_of_replicas":0
		},
		"mappings": {
			"_doc": {
				"properties": {
					"epochtime_loaded": { "type": "date" },
					"version": {"type": "nested"},
					"book": {
						"type": "keyword", 
                    "index": false
					},
					"chapter_no": {"type": "integer"},
					"verse_no": {"type": "integer"},
					"verse" : {"type": "text",
					  "index": true
					},
					"es_id" : {"type": "text","index": false},
					"utctime_loaded": {"type":"date"}
				}
			}
		}
	}`
	exists, err := client.IndexExists(indexName).Do(ctx)
	checkErr(err)
	if exists {
		fmt.Printf("Index %s already exists. Set environment variable DELETE_INDEX to true to delete")
		return
	}
	createIndex, err := client.CreateIndex(indexName).BodyString(mapping).Do(ctx)
	checkErr(err)
	fmt.Println("Index created ", createIndex.Acknowledged)
	if !createIndex.Acknowledged {
		// Not acknowledged
	}

}
