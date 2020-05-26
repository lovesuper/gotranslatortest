package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	_ "github.com/lib/pq"
)

/*
VARS:

export DBTRNSLT_DB_HOST=
export DBTRNSLT_DB_PORT=
export DBTRNSLT_DB_USER=
export DBTRNSLT_DB_PASSWORD=
export DBTRNSLT_DB_NAME=
export DBTRNSLT_GCLOUD_TOKEN=
export DBTRNSLT_WORKERS_COUNT=
export DBTRNSLT_OFFSET_DEFAULT_STEP=
export DBTRNSLT_GOOGLE_TRANSLATE_API=
*/

var offsetDefaultStep, _ = strconv.Atoi(os.Getenv("DBTRNSLT_OFFSET_DEFAULT_STEP"))
var currentOffset int = 0 - offsetDefaultStep
var targetLangsPool = []string{"ru", "fr", "zh", "it", "jp", "kr", "pt", "sw", "es", "de"}

type googleTranslateRequest struct {
	Q      string `json:"q"`
	Source string `json:"source"`
	Target string `json:"target"`
	Format string `json:"format"`
}

var psqlInfo = fmt.Sprintf(
	"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
	os.Getenv("DBTRNSLT_DB_HOST"),
	os.Getenv("DBTRNSLT_DB_PORT"),
	os.Getenv("DBTRNSLT_DB_USER"),
	os.Getenv("DBTRNSLT_DB_PASSWORD"),
	os.Getenv("DBTRNSLT_DB_NAME"),
)

type translatedTextList []translatedText

type googleTranslateResponse struct {
	Data translatedData `json:"data"`
}

type translatedData struct {
	Translations translatedTextList `json:"translations"`
}

type translatedText struct {
	TranslatedText string `json:"translatedText"`
}

func translationsWorker(workerID int, lock *sync.RWMutex, db *sql.DB, rowsCount int, wg *sync.WaitGroup) {
	lock.Lock()
	currentOffset += offsetDefaultStep
	var offsetCache = currentOffset
	lock.Unlock()

	fmt.Printf("Starting new goroutine. #%d does OFFSET %d\n", workerID, offsetCache)

	defer wg.Done()
	if offsetCache > rowsCount+offsetDefaultStep {
		return
	}

	// sql, _, _ :=
	// 	(sq.Select("cities.id", "cities.name_en").
	// 		From("cities").
	// 		Join("countries ON cities.country_id=countries.id").
	// 		Limit(uint64(offsetDefaultStep)).
	// 		OrderBy("name_en").
	// 		Offset(uint64(offsetCache))).ToSql()

	// sql, _, _ := (sq.Select("id", "name_en").
	// 	From("countries").
	// 	Limit(uint64(offsetDefaultStep)).
	// 	OrderBy("name_en").
	// 	Offset(uint64(offsetCache))).ToSql()

	sql, _, _ := (sq.Select("id", "name_en").
		From("disciplines").
		Limit(uint64(offsetDefaultStep)).
		Where("is_classic").
		OrderBy("name_en").
		Offset(uint64(offsetCache))).ToSql()

	startTranslations(db, sql)
	if offsetCache < rowsCount+offsetDefaultStep {
		fmt.Println("Here we go again with workerID:", workerID)
		wg.Add(1)
		translationsWorker(workerID, lock, db, rowsCount, wg)
	}
}

func startTranslations(db *sql.DB, sql string) {
	rows, err := db.Query(sql)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var id string
	var nameEn string
	// table := "cities"
	// table := "countries"
	table := "disciplines"
	for rows.Next() {
		err := rows.Scan(&id, &nameEn)
		if err != nil {
			log.Fatal(err)
		}
		translationsMap := getTranslations(targetLangsPool, nameEn)
		updateRow(db, table, id, translationsMap)
	}
}

func main() {
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	var wg sync.WaitGroup

	sql, _, _ := sq.Select("COUNT(id)").From("disciplines").Where("is_classic").ToSql()
	// sql, _, _ := sq.Select("COUNT(id)").From("countries").ToSql()
	// sql, _, _ := sq.Select("COUNT(id)").From("cities").ToSql()

	var rowsCount int
	db.QueryRow(sql).Scan(&rowsCount)
	fmt.Printf("ALL: %d\n", rowsCount)
	var lock sync.RWMutex
	workersCount, _ := strconv.Atoi(os.Getenv("DBTRNSLT_WORKERS_COUNT"))
	for workerID := 0; workerID <= workersCount; workerID++ {
		wg.Add(1)
		go translationsWorker(workerID, &lock, db, rowsCount, &wg)
		time.Sleep(time.Duration(50) * time.Millisecond)
	}

	wg.Wait()
}

func updateRow(db *sql.DB, table string, rowUID string, translationsMap map[string]string) {
	var translationsMapInterfaced map[string]interface{} = make(map[string]interface{})
	for key, value := range translationsMap {
		translationsMapInterfaced[key] = value
	}
	updateQuery :=
		(sq.Update(table).
			SetMap(translationsMapInterfaced).
			Where(sq.Eq{"id": rowUID}))

	var sql, args, _ = updateQuery.PlaceholderFormat(sq.Dollar).ToSql()
	var _, err = db.Exec(sql, args...)
	if err != nil {
		log.Fatal(err)
	}
}

func getTranslations(langPool []string, sourceString string) (translationsMap map[string]string) {
	translationsMap = make(map[string]string)
	var googleISO string
	for _, lang := range langPool {
		if lang == "jp" {
			googleISO = "jpn"
		} else if lang == "kr" {
			googleISO = "kor"
		} else {
			googleISO = lang
		}

		translatedString := getTranslatedString(sourceString, "en", googleISO)
		translatedString = strings.Title(translatedString)
		// var translatedString = "Test"
		translationsMap[fmt.Sprintf("name_%s", lang)] = translatedString
	}

	return
}

func getTranslatedString(stringToTranslate string, sourceLang string, targetLang string) string {
	client := &http.Client{}
	requestJSON, _ := json.Marshal(&googleTranslateRequest{
		Q:      stringToTranslate,
		Source: sourceLang,
		Target: targetLang,
		Format: "text",
	})
	request, err := http.NewRequest("POST", os.Getenv("DBTRNSLT_GOOGLE_TRANSLATE_API"), bytes.NewBuffer(requestJSON))
	request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", os.Getenv("DBTRNSLT_GCLOUD_TOKEN")))
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		panic(err)
	}

	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	googleResponse := googleTranslateResponse{}
	_ = json.Unmarshal([]byte(body), &googleResponse)

	return googleResponse.Data.Translations[0].TranslatedText
}
