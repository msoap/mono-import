/*
Importing CSV data from monobank to SQLite DB
Usage:

	go run mono-import.go -db=mono.db mono_*.csv
*/
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

const (
	centsCoef     = 100
	rateCoef      = 100_000
	csvDateFormat = "02.01.2006 15:04:05"
)

type record struct {
	CreatedAt  time.Time `db:"created_at"`
	Title      string    `db:"title"`
	MCC        int       `db:"mcc"`
	Amount     int       `db:"amount"`      // in UAH * 100 (kopecks)
	AmountOrig int       `db:"amount_orig"` // in original currency (USD/EUR): V * 100 (cents)
	Currency   string    `db:"currency"`    // UAH/USD/EUR
	Exchange   int       `db:"exchange"`    // exchange rate: V * 100000
	Commission int       `db:"commission"`  // in UAH * 100
	Cashback   int       `db:"cashback"`    // in UAH * 100
	Rest       int       `db:"rest"`        // in UAH * 100
}

func main() {
	dbName := ""
	flag.StringVar(&dbName, "db", "mono.db", "SQLite DB name")
	flag.Parse()
	fmt.Printf("Importing to %s\n", dbName)

	allData := readFiles(flag.Args())

	n, err := saveToDB(dbName, allData)
	if err != nil {
		log.Fatalf("Error saving to DB %s: %s", dbName, err)
	}

	fmt.Printf("Imported %d (from %d) records\n", n, len(allData))
}

func readFiles(files []string) []record {
	allData := []record{}
	dupl := map[string]bool{}

	for _, filename := range files {
		fmt.Printf("Importing from %s\n", filename)

		// read CSV file
		data, err := readCSV(filename)
		if err != nil {
			log.Fatalf("Error reading CSV file %s: %s", filename, err)
		}
		if len(data) <= 1 {
			log.Printf("Empty CSV file: %s", filename)
			continue
		}

		recLen := len(data[0])
		// remove header
		data = data[1:]

		for i, row := range data {
			if len(row) < recLen {
				continue
			}

			rec := parseRecord(row)
			allData = append(allData, rec)

			key := rec.CreatedAt.Format(csvDateFormat) + rec.Title + strconv.Itoa(rec.Amount)
			if dupl[key] {
				log.Fatalf("Duplicate record %d (%s): %#v", i, filename, rec)
			}
			dupl[key] = true
		}
	}

	return allData
}

func readCSV(filename string) ([][]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("Error opening file %s: %s", filename, err)
	}
	defer f.Close()

	csvr := csv.NewReader(f)
	csvr.FieldsPerRecord = -1 // variable number of fields

	return csvr.ReadAll()
}

func parseRecord(row []string) record {
	// CSV header:
	// "Дата i час операції","Деталі операції",MCC,"Сума в валюті картки (UAH)","Сума в валюті операції",Валюта,Курс,"Сума комісій (UAH)","Сума кешбеку (UAH)","Залишок після операції"

	r := record{}

	// parse CreatedAt
	createdAt, err := time.Parse(csvDateFormat, row[0])
	if err != nil {
		log.Fatalf("Error parsing CreatedAt %s: %s", row[0], err)
	}
	r.CreatedAt = createdAt

	// parse Title
	r.Title = row[1]

	// parse MCC
	r.MCC = parseAsInt(row[2], 1)

	// parse Amount
	r.Amount = parseAsInt(row[3], centsCoef)

	// parse AmountOrig
	r.AmountOrig = parseAsInt(row[4], centsCoef)

	// parse Currency
	r.Currency = row[5]

	// parse Exchange
	r.Exchange = parseAsInt(row[6], rateCoef)

	// parse Commission
	r.Commission = parseAsInt(row[7], centsCoef)

	// parse Cashback
	r.Cashback = parseAsInt(row[8], centsCoef)

	// parse Rest
	r.Rest = parseAsInt(row[9], centsCoef)

	return r
}

func parseAsInt(s string, coef int) int {
	if s == "—" || s == "-" || s == "" {
		return 0
	}

	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		log.Fatalf("Error parsing %s to float: %s", s, err)
	}
	return int(v * float64(coef))
}

func saveToDB(dbName string, data []record) (int, error) {
	db, err := sqlx.Open("sqlite3", dbName)
	if err != nil {
		return 0, fmt.Errorf("Error opening DB %s: %s", dbName, err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			log.Fatalf("Error closing DB: %s", err)
		}
	}()

	// create table
	if _, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS mono (
		created_at  DATETIME,
		title       TEXT,
		mcc         INTEGER,
		amount      DECIMAL(10,2),
		amount_orig DECIMAL(10,2),
		currency    TEXT,
		exchange    DECIMAL(10,5),
		commission  DECIMAL(10,2),
		cashback    DECIMAL(10,2),
		rest        DECIMAL(10,2),

		UNIQUE (created_at, title, amount)
	)`); err != nil {
		return 0, fmt.Errorf("Error creating table: %s", err)
	}

	// insert data
	sqlQuery := `
		INSERT INTO mono (
			created_at,
			title,
			mcc,
			amount,
			amount_orig,
			currency,
			exchange,
			commission,
			cashback,
			rest
		) VALUES (
			:created_at,
			:title,
			:mcc,
			:amount / 100.0,
			:amount_orig / 100.0,
			:currency,
			:exchange / 100000.0,
			:commission / 100.0,
			:cashback / 100.0,
			:rest / 100.0
		)
		ON CONFLICT(created_at, title, amount) DO NOTHING
	`
	cnt := 0
	for _, rec := range data {
		// insert record
		res, err := db.NamedExec(sqlQuery, rec)
		if err != nil {
			return 0, fmt.Errorf("Error inserting record %#v: %s", rec, err)
		}

		n, err := res.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("Error getting rows affected: %s", err)
		}

		cnt += int(n)
	}

	return cnt, nil
}
